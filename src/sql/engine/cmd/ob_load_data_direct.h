// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#pragma once

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
#include <atomic>
#include <queue>

namespace oceanbase
{
namespace sql
{
constexpr int64_t TASK_SIZE = (1LL << 28); // 512M
constexpr int64_t MEM_BUFFER_SIZE = (1LL << 30); // 1G
constexpr int64_t FILE_BUFFER_SIZE = (2LL << 20); // 2M
constexpr int64_t N_CPU = 8;

class ObLoadDataBuffer
{
public:
  ObLoadDataBuffer();
  ~ObLoadDataBuffer();
  void reuse();
  void reset();
  int create(int64_t capacity, int task_id);
  int squash();
  OB_INLINE char *data() const { return data_; }
  OB_INLINE char *begin() const { return data_ + begin_pos_; }
  OB_INLINE char *end() const { return data_ + end_pos_; }
  OB_INLINE bool empty() const { return end_pos_ == begin_pos_; }
  OB_INLINE int64_t get_offset() const { return file_begin_pos_ + end_pos_ - begin_pos_; }
  OB_INLINE bool is_compete() const { return file_end_pos_ <= file_begin_pos_; }
  OB_INLINE int64_t get_data_size() const { return end_pos_ - begin_pos_; }
  OB_INLINE int64_t get_remain_size() const { return capacity_ - end_pos_; }
  OB_INLINE int64_t get_file_pos() const { return file_begin_pos_; }
  OB_INLINE bool should_step() const { return step_; }
  OB_INLINE void set_step(bool flag) { step_ = flag; } 
  OB_INLINE void consume(int64_t size)
  {
    begin_pos_ += size;
    file_begin_pos_ += size;
  }
  OB_INLINE void produce(int64_t size) { end_pos_ += size; }
private:
  common::ObArenaAllocator allocator_;
  char *data_;
  int64_t begin_pos_;
  int64_t end_pos_;
  int64_t file_begin_pos_;
  int64_t file_end_pos_;
  int64_t capacity_;
  bool step_;
};

class ObLoadSequentialFileReader
{
public:
  ObLoadSequentialFileReader();
  ~ObLoadSequentialFileReader();
  int open(const ObString &filepath);
  int read_next_buffer(ObLoadDataBuffer &buffer);
private:
  common::ObFileReader file_reader_;
  int64_t offset_;
  bool is_read_end_;
};

class ObLoadCSVPaser
{
public:
  ObLoadCSVPaser();
  ~ObLoadCSVPaser();
  void reset();
  int init(const ObDataInFileStruct &format, int64_t column_count,
           common::ObCollationType collation_type);
  int get_next_row(ObLoadDataBuffer &buffer, const common::ObNewRow *&row);
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
  DECLARE_TO_STRING;
public:
  common::ObArenaAllocator allocator_;
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
  bool is_inited_;
};

class ObLoadExternalSort
{
public:
  ObLoadExternalSort();
  ~ObLoadExternalSort();
  int init(const share::schema::ObTableSchema *table_schema, int64_t mem_size,
           int64_t file_buf_size);
  int append_row(const ObLoadDatumRow &datum_row);
  uint64_t get_size() const { return size_;};
  int close();
  int get_next_row(const ObLoadDatumRow *&datum_row);
  ObLoadDatumRowCompare& get_compare() { return compare_; };
private:
  common::ObArenaAllocator allocator_;
  common::ObSpinLock lock_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObLoadDatumRowCompare compare_;
  storage::ObExternalSort<ObLoadDatumRow, ObLoadDatumRowCompare> external_sort_;
  uint64_t size_ = 0;
  bool is_closed_;
  bool is_inited_;
};

class ObLoadSSTableWriter
{
public:
  ObLoadSSTableWriter();
  ~ObLoadSSTableWriter();
  int init(const share::schema::ObTableSchema *table_schema);
  int append_row(const ObLoadDatumRow &datum_row);
  int close();
private:
  int init_sstable_index_builder(const share::schema::ObTableSchema *table_schema);
  int init_macro_block_writer(const share::schema::ObTableSchema *table_schema);
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
  blocksstable::ObMacroBlockWriter macro_block_writer_;
  blocksstable::ObDatumRow datum_row_;
  bool is_closed_;
  bool is_inited_;
};

class ObLoadController : public share::ObThreadPool {
public:
  ObLoadController();
  ~ObLoadController();
public:
  int init(ObLoadDataStmt *load_stmt,
          ObLoadSequentialFileReader* file_reader,
          const ObTableSchema *table_schema);
  virtual void run1();

  int get_res() const { return ret_.load();}
  
  int32_t getTask();
  int next_row_init();
  int get_next_row(const ObLoadDatumRow *&datum_row);
  ObLoadDatumRowCompare& get_compare() { return external_sorts_[0].get_compare();};
  uint64_t get_size(int index) { return external_sorts_[index].get_size(); };
private:
  ObLoadCSVPaser csv_parser_;
  ObLoadSequentialFileReader *file_reader_;
  ObLoadExternalSort external_sorts_[N_CPU]; 
  // ObLoadExternalSort external_sort_;
  int pos_ = 0;
  const ObLoadDatumRow* pre_sorts_[N_CPU];
  ObLoadDataStmt *load_stmt_;
  ObLoadDatumRowCompare compare_;
  const ObTableSchema *table_schema_;
  
  common::ObSpinLock lock_;
  std::atomic<int32_t> task_id_ = {0};
  std::atomic<int32_t> finished_tasks_ = {8};
  std::atomic<int> ret_ = {0};
};

class ObLoadDataDirect : public ObLoadDataBase
{
public:
  ObLoadDataDirect();
  virtual ~ObLoadDataDirect();
  int execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt) override;
private:
  int inner_init(ObLoadDataStmt &load_stmt);
  int do_load();
private:
  ObLoadCSVPaser csv_parser_;
  ObLoadSequentialFileReader file_reader_;
  ObLoadController controller_;
  ObLoadSSTableWriter sstable_writer_;
};

} // namespace sql
} // namespace oceanbase