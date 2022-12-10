// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#pragma once

#include <dirent.h>
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <atomic>
#include <libaio.h>
#include <unistd.h>
#include "lib/file/ob_file.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "share/ob_thread_pool.h"
#include "lib/lock/ob_spin_lock.h"
#include "common/ob_clock_generator.h"

static constexpr int64_t FILE_DATA_BUFFER_SIZE = (2LL << 20); // 2M
static constexpr int64_t DATA_BUFFER_SIZE = (100LL << 20); // 100M
static constexpr int64_t PK_MIN = 0;
static constexpr int64_t PK_MAX = 300000000;
static constexpr int64_t PK_SPAN = (1LL << 18);
static constexpr int64_t PK1_BITE = 18;
static constexpr int64_t PK2_BITE = 3; 
static constexpr int64_t U_INT = 11;
static constexpr int64_t MASK = (1LL << U_INT) - 1; 
static constexpr int64_t PARTITION_NUM = PK_MAX / PK_SPAN + 1 - PK_MIN / PK_SPAN;
static const char * PARTITION_DIR = "./load-partition/";

static constexpr int64_t TASK_SIZE = (128LL << 20); // 128M
static constexpr int64_t COMPRESS_BUFF_SIZE = (1LL << 20); // 1M
static constexpr int64_t MEM_BUFFER_SIZE = (1LL << 30); // 1G
static constexpr int64_t SORT_BUFFER_SIZE = 4 * (1LL << 30); // 4G
static constexpr int64_t N_CPU = 16;
static constexpr int64_t N_LOCK_SHARD = PARTITION_NUM;

static constexpr ObCompressorType COMPRESS_TYPE = ZSTD_1_3_8_COMPRESSOR;
// additional error code `OB_END_OF_PARTITION`,
// which means this partition has been read through,
// and next partition file is needed to be open.
static constexpr int OB_BUFFER_FULL = -4039;
static constexpr int OB_END_OF_PARTITION = -4745;
static constexpr int OB_TASK_FINISH = -4011;

namespace oceanbase
{
namespace sql
{

class ObLoadDataBuffer
{
public:
  ObLoadDataBuffer();
  ~ObLoadDataBuffer();
  void reuse();
  void reset();
  int create(int64_t capacity);
  int squash();
  OB_INLINE char *data() const { return data_; }
  OB_INLINE char *begin() const { return data_ + begin_pos_; }
  OB_INLINE char *end() const { return data_ + end_pos_; }
  OB_INLINE bool empty() const { return end_pos_ == begin_pos_; }
  OB_INLINE int64_t get_data_size() const { return end_pos_ - begin_pos_; }
  OB_INLINE int64_t get_remain_size() const { return capacity_ - end_pos_; }
  OB_INLINE virtual void consume(int64_t size) { begin_pos_ += size; }
  OB_INLINE void produce(int64_t size) { end_pos_ += size; }
protected:
  common::ObArenaAllocator allocator_;
  char *data_;
  int64_t begin_pos_;
  int64_t end_pos_;
  int64_t capacity_;
};

class ObLoadFileDataBuffer : public ObLoadDataBuffer
{
public:
  ObLoadFileDataBuffer() = default;
  ~ObLoadFileDataBuffer() = default;
public:
  int create(int64_t capacity, int task_id);
  OB_INLINE int64_t get_offset() const { return file_begin_pos_ + end_pos_ - begin_pos_; }
  OB_INLINE bool is_compete() const { return file_end_pos_ < file_begin_pos_; }
  OB_INLINE int64_t get_file_pos() const { return file_begin_pos_; }
  OB_INLINE bool should_step() const { return step_; }
  OB_INLINE void set_step(bool flag) { step_ = flag; }
  OB_INLINE void consume(int64_t size) override {
    begin_pos_ += size;
    file_begin_pos_ += size;
  }
private:
  int64_t file_begin_pos_ = 0;
  int64_t file_end_pos_ = 0;
  bool step_ = false;
};

class ObLoadSequentialFileReader
{
public:
  ObLoadSequentialFileReader();
  ~ObLoadSequentialFileReader();
  int open(const ObString &filepath);
  void close() { file_reader_.close(); offset_ = 0; }
  int read_next_buffer(ObLoadDataBuffer &buffer, int64_t offset = -1);
private:
  common::ObFileReader file_reader_;
  int64_t offset_;
  bool is_read_end_;
};

class ObLoadAioAppender
{
public:
  ObLoadAioAppender() {};
  ~ObLoadAioAppender() {};
  int create(const ObString &filepath) {
    max_task_size_ = 1;
    cur_task_size_ = 0;
    offset_ = 0;
    memset(&ctx_, 0, sizeof(ctx_));
    if (io_setup(128, &ctx_) != 0) {
        return -1;
    }
    if ((fd_ = open(filepath.ptr(), O_CREAT | O_WRONLY, 0644)) < 0) {
      return -2;
    }
    return 0;
  }
  int aio_close() {
    struct io_event e[max_task_size_];
    if (io_getevents(ctx_, cur_task_size_, max_task_size_, e, NULL) < 0) {
      return -1;
    }
    close(fd_);
    io_destroy(ctx_);
    return 0;
  }
  int append(void *buff, int64_t size) {
    struct iocb io,*p=&io;
    struct io_event e, pre_event[max_task_size_];
    struct timespec timeout;
    int return_size = 0;
    struct timeval tv;
    gettimeofday(&tv,NULL);
    int64_t stamp1 = tv.tv_sec * 1000000 + tv.tv_usec;
    if (cur_task_size_ == max_task_size_) {
      return_size = io_getevents(ctx_, 1, max_task_size_, pre_event, NULL);
    } else {
      return_size = io_getevents(ctx_, 0, max_task_size_, pre_event, NULL);
    }
    gettimeofday(&tv,NULL);
    int64_t stamp2 = tv.tv_sec * 1000000 + tv.tv_usec;
    LOG_INFO("write waite time.", K(stamp2 - stamp1), K(cur_task_size_), K(return_size));
    cur_task_size_ -= return_size;
    io_prep_pwrite(&io, fd_, buff, size, offset_);
    io.data = buff;
    if (io_submit(ctx_, 1, &p) != 1) {
        return -2;
    }
    gettimeofday(&tv,NULL);
    int64_t stamp3 = tv.tv_sec * 1000000 + tv.tv_usec;
    LOG_INFO("write submite waite time.", K(stamp3 - stamp2));
    offset_ += size;
    cur_task_size_++;
    return 0;
  }
private:
  io_context_t ctx_;
  int64_t max_task_size_;
  int64_t cur_task_size_;
  int64_t offset_;
  int32_t fd_;
};

class ObLoadAioReader 
{
public:
  ObLoadAioReader() {}
  ~ObLoadAioReader() {}
  int aio_open(const ObString &filepath) {
    allocator_.set_tenant_id(MTL_ID());
    capacity_ = FILE_DATA_BUFFER_SIZE * 2;
    offset_ = 0;
    begin_ = 0;
    end_ = 0;
    is_iter_end_ = false;
    int ret = OB_SUCCESS;
    memset(&ctx_, 0, sizeof(ctx_));
    if (io_setup(1, &ctx_) != 0) {
        return -1;
    }
    if ((fd_ = open(filepath.ptr(), O_RDONLY, 0644)) < 0) {
      return -2;
    }
    struct stat file_stat;
    stat(filepath.ptr(), &file_stat);
    max_size_ = file_stat.st_size;
    if (OB_ISNULL(buf_ = static_cast<char*>(allocator_.alloc(capacity_)))) {
      LOG_WARN("");
    }
    uint64_t extral = ((uint64_t)buf_) % 512;
    if (extral != 0) {
      buf_ += (512 - extral);
      capacity_ -= 512;
    }
    return ret;
  }
  void reset() {
    begin_ = 0;
    end_ = 0;
    is_iter_end_ = false;
  }
  int read_next_buffer(ObLoadDataBuffer &buffer) {
    struct io_event pre_event;
    struct timeval tv;
    gettimeofday(&tv,NULL);
    int64_t stamp1 = tv.tv_sec * 1000000 + tv.tv_usec;
    
    if (io_getevents(ctx_, 1, 1, &pre_event, NULL) <= 0) {
      return -1;
    }

    gettimeofday(&tv, NULL);
    int64_t stamp2 = tv.tv_sec * 1000000 + tv.tv_usec;
    LOG_INFO("aio read waste time.", K(pre_event.res), K(pre_event.res2), K(stamp2 - stamp1));
    
    int64_t return_size = last_read_size_;
    if (return_size == 0) {
      return OB_ITER_END;
    }
    end_ += return_size;
    offset_ += return_size;
    int64_t read_size = buffer.get_remain_size();
    read_size = read_size > end_ ? end_ : read_size;  
    MEMCPY(buffer.end(), buf_, read_size);
    buffer.produce(read_size);
    MEMCPY(buf_, buf_ + read_size, end_ - read_size);
    end_ -= read_size;

    gettimeofday(&tv,NULL);
    int64_t stamp3 = tv.tv_sec * 1000000 + tv.tv_usec;
    struct iocb *p = &io_;
    last_read_size_ = FILE_DATA_BUFFER_SIZE > (max_size_ - offset_) ? max_size_ - offset_ : FILE_DATA_BUFFER_SIZE;

    if ((last_read_size_ % 512 != 0) || (((uint64_t)buf_ + end_) % 512 != 0)) {
      LOG_WARN("not ali.", K(read_size), K(end_));
    }
    io_prep_pread(&io_, fd_, buf_ + end_, FILE_DATA_BUFFER_SIZE, offset_);
    if (io_submit(ctx_, 1, &p) != 1) {
      return -2;
    }
    gettimeofday(&tv,NULL);
    int64_t stamp4 = tv.tv_sec * 1000000 + tv.tv_usec;
    LOG_INFO("read submite waste time.", K(stamp4 - stamp3));
    return 0;
  }
  int set_offset_and_pread(int64_t offset) {
    offset_ = offset;
    struct iocb *p = &io_;
    if (max_size_ <= offset)  {
      return OB_ITER_END;
    }
    last_read_size_ = FILE_DATA_BUFFER_SIZE > (max_size_ - offset_)? (max_size_ - offset_) : FILE_DATA_BUFFER_SIZE;
    io_prep_pread(&io_, fd_, buf_, FILE_DATA_BUFFER_SIZE, offset_);
    if (io_submit(ctx_, 1, &p) != 1) {
      return -1;
    }
    return 0;
  }
  void aio_close() {
    close(fd_);
    io_destroy(ctx_);
  } 
private:
  common::ObArenaAllocator allocator_;
  char *buf_;
  struct iocb io_;
  io_context_t ctx_;
  int64_t last_read_size_;
  int64_t begin_;
  int64_t end_;
  int64_t offset_;
  int64_t max_size_;
  int64_t capacity_;
  bool is_iter_end_;
  int32_t fd_;
};

class ObLoadCSVPaser
{
public:
  ObLoadCSVPaser();
  ~ObLoadCSVPaser();
  void reset();
  int init(const ObDataInFileStruct &format, int64_t column_count,
           common::ObCollationType collation_type);
  int get_next_row(ObLoadFileDataBuffer &buffer, const common::ObNewRow *&row);
private:
  struct UnusedRowHandler
  {
    int operator()(common::ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line)
    {
      UNUSED(fields_per_line);
      return OB_SUCCESS;
    }
  };
private:
  common::ObArenaAllocator allocator_;
  common::ObCollationType collation_type_;
  ObCSVGeneralParser csv_parser_;
  common::ObNewRow row_;
  UnusedRowHandler unused_row_handler_;
  common::ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records_;
  bool is_inited_;
};

class ObLoadDatumRowAllocator
{
public:
  static common::ObArenaAllocator &get_allocator() {
    thread_local common::ObArenaAllocator allocator(ObModIds::OB_SQL_LOAD_DATA);
    return allocator;
  }
};

class ObLoadDatumRow
{
  OB_UNIS_VERSION(1);
public:
  ObLoadDatumRow();
  ~ObLoadDatumRow();
  void reset();
  int init(int64_t capacity);
  int64_t get_deep_copy_size() const;
  int deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len, int64_t &pos);
  OB_INLINE bool is_valid() const { return count_ > 0 && nullptr != datums_; }
  OB_INLINE int64_t get_bits(int index) { return (((datums_[0].get_int() << PK2_BITE) + datums_[1].get_int()) >> index) & MASK; }
  DECLARE_TO_STRING;
public:
  // common::ObArenaAllocator allocator_;
  int64_t capacity_;
  int64_t count_;
  blocksstable::ObStorageDatum *datums_;
};

class ObLoadDatumRowCompare
{
public:
  ObLoadDatumRowCompare();
  ~ObLoadDatumRowCompare();
  int init(int64_t rowkey_column_num, const blocksstable::ObStorageDatumUtils *datum_utils);
  bool operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs);
  int get_error_code() const { return result_code_; }
public:
  int result_code_;
private:
  int64_t rowkey_column_num_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  blocksstable::ObDatumRowkey lhs_rowkey_;
  blocksstable::ObDatumRowkey rhs_rowkey_;
  bool is_inited_;
};

class ObLoadRowCaster
{
public:
  ObLoadRowCaster();
  ~ObLoadRowCaster();
  int init(const share::schema::ObTableSchema *table_schema,
           const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
  int get_casted_row(const common::ObNewRow &new_row, const ObLoadDatumRow *&datum_row);
private:
  int init_column_schemas_and_idxs(
    const share::schema::ObTableSchema *table_schema,
    const common::ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list);
  int cast_obj_to_datum(const share::schema::ObColumnSchemaV2 *column_schema,
                        const common::ObObj &obj, blocksstable::ObStorageDatum &datum);
private:
  common::ObArray<const share::schema::ObColumnSchemaV2 *> column_schemas_;
  common::ObArray<int64_t> column_idxs_; // Mapping of store columns to source data columns
  int64_t column_count_;
  common::ObCollationType collation_type_;
  ObLoadDatumRow datum_row_;
  common::ObArenaAllocator cast_allocator_;
  common::ObTimeZoneInfo tz_info_;
  common::ObDataTypeCastParams cast_params_;
  bool is_inited_;
};

class ObLoadSSTableWriter
{
public:
  ObLoadSSTableWriter();
  ~ObLoadSSTableWriter();
  int init(const share::schema::ObTableSchema *table_schema);
  int reuse_macro_block_writer(const int index, const int64_t task_id, const share::schema::ObTableSchema *table_schema);
  int close_macro_block_writer(const int index);
  int append_row(const int index, const ObLoadDatumRow &datum_row);
  int close();
private:
  int init_sstable_index_builder(const share::schema::ObTableSchema *table_schema);
  // int init_macro_block_writer(int index, int task_id,const share::schema::ObTableSchema *table_schema);
  int create_sstable();
private:
  common::ObTabletID tablet_id_;
  storage::ObTabletHandle tablet_handle_;
  share::ObLSID ls_id_;
  storage::ObLSHandle ls_handle_;
  int64_t rowkey_column_num_;
  int64_t extra_rowkey_column_num_;
  int64_t column_count_;
  storage::ObITable::TableKey table_key_;
  blocksstable::ObSSTableIndexBuilder sstable_index_builder_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  blocksstable::ObMacroBlockWriter macro_block_writers_[N_CPU];
  blocksstable::ObDatumRow datum_rows_[N_CPU];
  bool is_closed_;
  bool is_inited_;
};

// TODO: data sketch
// TODO: parallel optimize
class ObPartitionWriter
{
public:
using DirectFileAppender = common::FileComponent::DirectFileAppender;
  typedef struct
  {
    int create(int64_t capacity, common::ObArenaAllocator* allocator, common::ObString file_path) {
      int ret = OB_SUCCESS;
      allocator_ = allocator;
      int64_t max_overflow_size = 0;
      if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(COMPRESS_TYPE, compressor_))) {
        STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), K(COMPRESS_TYPE));
      } else if (compressor_->get_max_overflow_size(capacity, max_overflow_size)) {
        LOG_WARN("fail to get max overflow size.", KR(ret));
      } else if (OB_ISNULL(buff_ = static_cast<char*>(allocator_->alloc(capacity)))) {
        LOG_WARN("fail to allocat in paertition reader.",KR(ret));
      } else if (OB_ISNULL(compress_buff_ = static_cast<char*>(allocator_->alloc(capacity * 2 + max_overflow_size + sizeof(int64_t))))) {
        LOG_WARN("fail to allocat in paertition reader.",KR(ret));
      } else if (OB_FAIL(appender_.create(file_path))) {
        LOG_WARN("fail to create partitoin file", KR(ret), K(file_path));
      } else {
        compress_capacity_ = capacity * 2 + max_overflow_size + sizeof(int64_t);
        capacity_ = capacity;
        offset_ = 0;
        compress_offset_ = 0;
      }
      return ret;
    }
    int append_row(const ObLoadDatumRow *&datum_row) {
      int ret = OB_SUCCESS;
      if (OB_FAIL(datum_row->serialize(buff_ , capacity_, offset_))) {
        LOG_WARN("datum row serialize fail.", KR(ret), K(offset_));
      } else if (capacity_ - offset_ > tail_size_) {
        
      } else if (OB_FAIL(compress())) {
        LOG_WARN("fail to compress block.", KR(ret));
      } else if (OB_FAIL(flush(false))) {
        LOG_WARN("false to flush compress buffer.", KR(ret));
      } else {
        offset_ = 0;
      }
      return ret;
    }
    int close() {
      int ret = OB_SUCCESS;
      if (offset_ != 0) {
        if (OB_FAIL(compress())) {
          LOG_WARN("compress error.", K(ret));
        }
      }
      if (compress_offset_ != 0) {
        if (OB_FAIL(flush(true))) {
          LOG_WARN("append flush fail.", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(appender_.aio_close())) {
          LOG_WARN("appender close error!", K(ret));
        }
      }
      return ret;
    };
    int flush(bool is_flush) {
      int ret = 0;
      int64_t flush_size = 0;
      if (OB_UNLIKELY(is_flush)) {
        flush_size = compress_offset_;
        if (OB_FAIL(appender_.append(compress_buff_, flush_size))) {
          LOG_WARN("fail to append tmp file block.", K(ret));
        } else {
          LOG_INFO("flush 1", K(flush_size), K(compress_offset_));
        }
      } else if (OB_UNLIKELY(sholud_flush())) {
        flush_size = capacity_;
        if (OB_FAIL(appender_.append(compress_buff_, flush_size))) {
          LOG_WARN("fail to append tmp file block.", K(ret));
        } else {
          LOG_INFO("flush 2", K(flush_size), K(compress_offset_));
        }
      }
      if (flush_size != 0) {
        MEMCPY(compress_buff_, compress_buff_ + flush_size, compress_offset_ - flush_size);
        compress_offset_ -= flush_size;
      }
      return ret;
    }
    OB_INLINE bool sholud_flush() { return compress_offset_ >= capacity_; }
    OB_INLINE char* get_compress_begin() { return compress_buff_ + compress_offset_; }
    OB_INLINE int64_t get_compress_remain_size() { return compress_capacity_ - compress_offset_; };
    OB_INLINE void compress_consume(int size) { compress_offset_ += size; }
    int compress() {
      int ret = OB_SUCCESS;
      int64_t size = 0;
      int64_t count_pos = compress_offset_;
      if (OB_FAIL(compressor_->compress(buff_, offset_, get_compress_begin() + sizeof(size), get_compress_remain_size() - sizeof(size), size))) {
        LOG_WARN("fail to compress tmp file block.", KR(ret));
      } else if (OB_FAIL(NS_::encode(compress_buff_, compress_capacity_, count_pos, size))) {
        LOG_WARN("fail to seriealize size of block.", KR(ret));
      } else {
        compress_consume(sizeof(size) + size);
      }
      return ret;
    }
    common::ObArenaAllocator *allocator_;
    char *buff_;
    char *compress_buff_;
    int64_t capacity_;
    
    // datum row buffer
    int64_t offset_ = 0;
    int64_t tail_size_ = 512;

    // compress buffer
    int64_t compress_capacity_;
    int64_t compress_offset_;
    ObLoadAioAppender appender_;
    common::ObCompressor *compressor_;
  } AppendBuffer;
  
  
  ObPartitionWriter() {}

  int init(const std::string& partition_directory) {
    int ret = OB_SUCCESS;
    allocator_.set_tenant_id(MTL_ID());
    for (int64_t i=0; i<PARTITION_NUM; i++) {
      std::string tmp = partition_directory + "/" + std::to_string(i);
      common::ObString file_path{ tmp.c_str() };
      if (OB_FAIL(buffers_[i].create(COMPRESS_BUFF_SIZE, &allocator_, file_path))) {
        LOG_WARN("fail to create partitoin file", KR(ret), K(file_path));
        break;
      }
    }
    return ret;
  }

  int append_row(const ObLoadDatumRow *&datum_row) {
    // TODO: select pk via StorageDatumUtils
    int64_t key = gen_key(datum_row->datums_[0].get_int());
    if (datum_row->datums_[1].get_int() > 16) {
      LOG_WARN("TEST FAIL OB_ERR_UNEXPECTED.", K(datum_row->datums_[1].get_int()));
      return OB_ERR_UNEXPECTED;
    }

    auto &lock = lock_[key % N_LOCK_SHARD];
    lock.lock();
    int ret = buffers_[key].append_row(datum_row);
    if (key == 1144) {
      LOG_INFO("1144 append offset.", K(buffers_[key].offset_));
    }
    lock.unlock();
    return ret;
  }

  int close() {
    int ret = OB_SUCCESS;
    for (auto &buffer: buffers_) {
      if (OB_FAIL(buffer.close())) {
        LOG_WARN("fail to close compress append buffer.", KR(ret));
      }
    }
    return ret;
  }

private:
  AppendBuffer buffers_[PARTITION_NUM];
  common::ObArenaAllocator allocator_;
  static OB_INLINE int64_t gen_key(int64_t pk) { return (pk - PK_MIN) / PK_SPAN; }
  common::ObSpinLock lock_[N_LOCK_SHARD];
};

// TODO: parallel optimize
class ObPartitionReader
{
  typedef struct
  {
    int create(int64_t capacity, common::ObArenaAllocator* allocator) {
      int ret = OB_SUCCESS;
      allocator_ = allocator;
      if (OB_ISNULL(compress_buff_ = static_cast<char*>(allocator_->alloc(capacity)))) {
        LOG_WARN("fail to allocat in paertition reader.",KR(ret));
      } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(COMPRESS_TYPE, compressor_))) {
        STORAGE_LOG(WARN, "Fail to get compressor, ", K(ret), K(COMPRESS_TYPE));
      } else {
        capacity_ = capacity;
        begin_ = 0;
        end_ = 0;
      }
      return ret;
    }
    int get_next_row(ObLoadDatumRow *datum_row) {
      int ret = OB_SUCCESS;
      int64_t size = 0;
      if (begin_ > end_) {
        ret = OB_ERR_UNEXPECTED;
      } else if (begin_ == end_) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(datum_row->deserialize(compress_buff_ + begin_, end_ - begin_, size))) {
        LOG_WARN("datum row deserialize fail.", KR(ret));
      } else {
        begin_ += size;
      }
      return ret;
    }
    int get_next_buffer(const char *&buff, const char *end) {
      int ret = OB_SUCCESS;
      int64_t size = 0;
      int64_t pos = 0;
      int64_t compress_size = 0;
      if (buff > end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected !", K(buff - end));
      } else if (buff == end) {
        ret = OB_ITER_END;
      } else if (OB_FAIL(NS_::decode(buff, sizeof(size), pos, size))) {
        LOG_WARN("decode size error.",K(buff), K(size));
      } else if (buff + size + sizeof(size) > end) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error.",K(buff), K(size), K(end));
      } else if (OB_FAIL(compressor_->decompress(buff + sizeof(size), size, compress_buff_ + begin_, capacity_ - begin_, compress_size))) {
        LOG_WARN("decompress error.", KR(ret));
      } else {
        end_ += compress_size;
        buff += size + sizeof(size);
      }
      return ret;
    }
    common::ObArenaAllocator *allocator_;
    char *compress_buff_;
    int64_t capacity_;
    int64_t begin_;
    int64_t end_;
    common::ObCompressor *compressor_;
  } ReadCompressBuffer;
public:
  ObPartitionReader() : allocator_(ObModIds::OB_SQL_LOAD_DATA) {}

  // read partitions includes [range_min, range_max]
  int init(std::string partition_directory, int64_t partition_id, int64_t capacity) {
    partition_directory_ = partition_directory;
    partition_id_ = partition_id;
    capacity_ = capacity;
    int ret = OB_SUCCESS;
    allocator_.set_tenant_id(MTL_ID());
    if (OB_FAIL(buffer_.create(DATA_BUFFER_SIZE))) {
      LOG_WARN("fail to create buffer during partition reader init", KR(ret));
    } else if (OB_FAIL(open_partition())) {
      LOG_WARN("fail to open next partition", KR(ret));
    } else if (OB_FAIL(read_next_buffer())) {
      LOG_WARN("fail to read next buffer", KR(ret));
    } else if (OB_FAIL(compress_buffer_.create(DATA_BUFFER_SIZE, &allocator_))) {
      LOG_WARN("fail to create a compress buffer.", KR(ret));
    }
    return ret;
  }

  int read(ObLoadDatumRow *&datum_row) {
    // TODO: select pk via StorageDatumUtils
    int ret;
    int64_t pos = 0;
    auto begin = buffer_.begin();
    auto end = buffer_.end();
    char *buff;
    if (OB_ISNULL(buff = static_cast<char *>(allocator_.alloc(sizeof(ObLoadDatumRow))))) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memery.", K(ret));
    } else if (OB_ISNULL(datum_row = new (buff) ObLoadDatumRow())) {
      ret = common::OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail replace datumn_row.", K(ret));
    } else if (OB_FAIL(compress_buffer_.get_next_row(datum_row))) {
      if (OB_UNLIKELY(ret == OB_ITER_END)) {
        ret = OB_SUCCESS;
        const char *str = buffer_.begin();
        if (begin == end) {
          ret = OB_ITER_END;
        } else if (begin > end) {
          ret = OB_ERR_UNEXPECTED;
        } else if (OB_FAIL(compress_buffer_.get_next_buffer(str, end))) {
          LOG_WARN("fail to get next compress buffer.", KR(ret));
        } else if (OB_FAIL(compress_buffer_.get_next_row(datum_row))) {
          LOG_WARN("fail to get next row.", KR(ret));
        } else {
          buffer_.consume(str - begin);
        }
      } else {
        LOG_WARN("fail to read next buffer with unexpected error", KR(ret));
      }
    }
    return ret;
  }

private:
  int read_next_buffer() {
    int ret = OB_SUCCESS;
    if (OB_FAIL(buffer_.squash())) {
      LOG_WARN("fail to squash buffer", KR(ret));
    } else if (OB_FAIL(file_reader_.read_next_buffer(buffer_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to read next buffer", KR(ret));
      } else {
        if (OB_UNLIKELY(!buffer_.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected incomplete data", KR(ret));
        } else {
          // this partition has been consumed
          ret = OB_END_OF_PARTITION;
        }
      }
    } else if (OB_UNLIKELY(buffer_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty buffer", KR(ret));
    }
    return ret;
  }

  int open_partition() {
    int ret = OB_SUCCESS;
    std::string file_path = partition_directory_ + "/" + std::to_string(partition_id_);
    ObString path{file_path.c_str()};
    if (OB_SUCC(file_reader_.open(path))) {
      LOG_WARN("fail to open partition file.", K(ret), K(partition_id));
    }
    LOG_INFO("open partition file successfully", K(partition_id_));
    return ret;
  }

private:
  common::ObArenaAllocator allocator_;
  std::string partition_directory_;
  int64_t partition_id_;
  int64_t capacity_;
  ReadCompressBuffer compress_buffer_;
  ObLoadSequentialFileReader file_reader_;
  ObLoadDataBuffer buffer_;
};

// TODO: clean code
class ObLoadDataSplitThreadPool : public share::ObThreadPool {
public:
  ObLoadDataSplitThreadPool() {}
  ~ObLoadDataSplitThreadPool() {}

public:
  int init(ObLoadDataStmt *load_stmt, const ObTableSchema *table_schema, std::string partition_directory);
  virtual void run1();

  int get_res() const { return ret_.load(); }
  int32_t get_task() { return task_id_.fetch_add(1); }
  int close() { return partition_writer_.close(); }

private:
  ObLoadDataStmt *load_stmt_;
  const ObTableSchema *table_schema_;

  ObLoadSequentialFileReader file_reader_;
  ObPartitionWriter partition_writer_;

  common::ObSpinLock lock_;
  std::atomic<int32_t> task_id_ = {0};
  std::atomic<int> ret_ = {0};
};

class ObSStableWriterThreadPool : public share::ObThreadPool {
public:
  ObSStableWriterThreadPool() {}
  ~ObSStableWriterThreadPool() {}
public:
  virtual void run1();
  int init(const ObTableSchema *table_schema, std::string partition_directory, int64_t min_id, int64_t max_id) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(sstable_writer_.init(table_schema))) {
      LOG_WARN("fail to init sstable.", K(ret));
    } else {
      table_schema_ = table_schema;
      partition_directory_ = partition_directory;
      max_id_ = max_id;
      task_id_.store(min_id);
    }
    return ret;
  }
  int close() { return sstable_writer_.close(); };
  int get_res() const { return ret_.load(); }
  int32_t get_task() { return task_id_.fetch_add(1); }
private:
  int64_t max_id_;
  const ObTableSchema *table_schema_;
  ObLoadSSTableWriter sstable_writer_;

  std::string partition_directory_;
  std::atomic<int64_t> task_id_ = {0};
  std::atomic<int> ret_ = {0};
};

class ObLoadDataSplitter
{
public:
  ObLoadDataSplitter() = default;

public:
  int init(ObLoadDataStmt &load_stmt, const ObTableSchema *table_schema, std::string partition_directory) {
    remove_directory(partition_directory.c_str());
    mkdir(PARTITION_DIR, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir(partition_directory.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    int ret = OB_SUCCESS;
    split_pool_ = new ObLoadDataSplitThreadPool();
    if (OB_FAIL(split_pool_->init(&load_stmt, table_schema, partition_directory))) {
      LOG_WARN("fail to init data split thread pool", KR(ret));
    }
    return ret;
  }

  int split() {
    struct timeval tv;
    gettimeofday(&tv,NULL);
    int64_t stamp1 = tv.tv_sec * 1000000 + tv.tv_usec;
    split_pool_->set_thread_count(N_CPU);
    split_pool_->set_run_wrapper(MTL_CTX());
    split_pool_->start();
    split_pool_->wait();
    gettimeofday(&tv,NULL);
    int64_t stamp2 = tv.tv_sec * 1000000 + tv.tv_usec;
    LOG_INFO("load data test time waste : stamp 1.", K(stamp2 - stamp1));
    int ret = OB_SUCCESS;
    if (OB_FAIL(split_pool_->close())) {
      LOG_WARN("fail to close split pool.", KR(ret));
    } else if (OB_FAIL(split_pool_->get_res())) {
      LOG_WARN("controller read err.", K(ret));
    }
    gettimeofday(&tv,NULL);
    int64_t stamp3 = tv.tv_sec * 1000000 + tv.tv_usec;
    LOG_INFO("load data test time waste : stamp 2.", K(stamp3 - stamp2));
    delete split_pool_;
    return ret;
  }

private:
  static int remove_directory(const char *const path) {
    DIR *const directory = opendir(path);
    if (directory) {
      char filename[1024];
      struct dirent *entry;
      while ((entry = readdir(directory))) {
        if (!strcmp(".", entry->d_name) || !strcmp("..", entry->d_name)) {
          continue;
        }
        sprintf(filename, "%s/%s", path, entry->d_name);
        remove(filename);
      }
      closedir(directory);
    }
    return remove(path);
  }

private:
  ObLoadDataSplitThreadPool *split_pool_{nullptr};
};

class ObLoadSort
{
public:
  ObLoadSort() : allocator_(ObModIds::OB_SQL_LOAD_DATA) {}

  int init(const share::schema::ObTableSchema *table_schema) {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
      ret = OB_INIT_TWICE;
      LOG_WARN("ObLoadSort init twice", KR(ret), KP(this));
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args", KR(ret), KP(table_schema));
    } else {
      allocator_.set_tenant_id(MTL_ID());
      const int64_t rowkey_column_num = table_schema->get_rowkey_column_num();
      ObArray<ObColDesc> multi_version_column_descs;
      if (OB_FAIL(table_schema->get_multi_version_column_descs(multi_version_column_descs))) {
        LOG_WARN("fail to get multi version column descs", KR(ret));
      } else if (OB_FAIL(datum_utils_.init(multi_version_column_descs, rowkey_column_num,
                                          is_oracle_mode(), allocator_))) {
        LOG_WARN("fail to init datum utils", KR(ret));
      } else if (OB_FAIL(compare_.init(rowkey_column_num, &datum_utils_))) {
        LOG_WARN("fail to init compare", KR(ret));
      } else {
        is_inited_ = true;
        pos_ = 0;
        datum_rows_.clear();
      }
    }
    return ret;
  }

  void reuse() {
    datum_rows_.clear();
    pos_ = 0;
  }

  int append_row(ObLoadDatumRow *datum_row) {
    if (datum_row == nullptr) {
      LOG_WARN("received null datum row");
      return OB_ERR_UNEXPECTED;
    }
    datum_rows_.push_back(datum_row);
    return OB_SUCCESS;
  }

  int close() {
    //std::sort(datum_rows_.begin(), datum_rows_.end(), compare_);
    int n = datum_rows_.size();
    copy_datum_rows_.resize(n, nullptr);
    for (int i = 0; i < PK1_BITE + PK2_BITE; i += U_INT) {
      memset(cnt, 0, sizeof(cnt));
      for (int j = 0; j != n; ++j)
        cnt[datum_rows_[j]->get_bits(i)]++;
      if (cnt[0] == n) continue;
      for (int sum = 0, j = 0; j != (1 << U_INT); ++j) {
        sum += cnt[j];
        cnt[j] = sum - cnt[j];
      }
      for (int j = 0; j != n; ++j) {
        int index =cnt[datum_rows_[j]->get_bits(i)];
        if (cnt[datum_rows_[j]->get_bits(i)] >= n) {
          LOG_WARN("TEST FAIL IN SORT 1.", K(index));
        }
        index = datum_rows_[j]->get_bits(i);
        if (datum_rows_[j]->get_bits(i) > MASK) {
          LOG_WARN("TEST FAIL IN SORT 2.", K(index));
        } 
        copy_datum_rows_[cnt[datum_rows_[j]->get_bits(i)]++] = datum_rows_[j];
      }
        
      std::swap(datum_rows_, copy_datum_rows_);
    }
    LOG_INFO("sorted item size", K(datum_rows_.size()));
    return OB_SUCCESS;
  }

  int get_next_row(ObLoadDatumRow *&datum_row) {
    if (pos_ >= datum_rows_.size())
      return OB_ITER_END;
    datum_row = datum_rows_[pos_++];
    return OB_SUCCESS;
  }

private:
  common::ObArenaAllocator allocator_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObLoadDatumRowCompare compare_;
  int64_t cnt[1 << U_INT];
  std::vector<ObLoadDatumRow *> copy_datum_rows_;
  std::vector<ObLoadDatumRow *> datum_rows_;
  int pos_ = 0;
  bool is_inited_ = false;
};

class ObLoadDataDirect : public ObLoadDataBase
{
    static const int64_t MEM_BUFFER_SIZE = (1LL << 30); // 1G
    static const int64_t FILE_BUFFER_SIZE = (2LL << 20); // 2M
public:
    ObLoadDataDirect() {}
    virtual ~ObLoadDataDirect() {}
    int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;
private:
    int inner_init(ObLoadDataStmt &load_stmt);
    int do_load();
private:
    ObLoadDataSplitter partition_splitter_;
    ObSStableWriterThreadPool sstable_writer_thread_pool_;
};
} // namespace sql
} // namespace oceanbase
