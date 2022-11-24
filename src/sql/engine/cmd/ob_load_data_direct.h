// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#pragma once

#include <dirent.h>
#include <unistd.h>
#include "map"
#include "fstream"
#include "lib/file/ob_file.h"
#include "lib/timezone/ob_timezone_info.h"
#include "sql/engine/cmd/ob_load_data_impl.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/ob_parallel_external_sort.h"
#include "storage/tx_storage/ob_ls_handle.h"

static constexpr int64_t FILE_BUFFER_SIZE = (200LL << 20); // 200M
static constexpr int64_t PARTITION_NUM = 400;
static constexpr int64_t PK_MIN = 1;
static constexpr int64_t PK_MAX = 300000000;
static constexpr int64_t PK_SPAN = (PK_MAX - PK_MIN + 1) / PARTITION_NUM;
static const char * PARTITION_DIR = "./partition/";


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
  OB_INLINE void consume(int64_t size) { begin_pos_ += size; }
  OB_INLINE void produce(int64_t size) { end_pos_ += size; }
private:
  common::ObArenaAllocator allocator_;
  char *data_;
  int64_t begin_pos_;
  int64_t end_pos_;
  int64_t capacity_;
};

class ObLoadSequentialFileReader
{
public:
  ObLoadSequentialFileReader();
  ~ObLoadSequentialFileReader();
  int open(const ObString &filepath);
  void close() { file_reader_.close(); offset_ = 0; }
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
  int close();
  int get_next_row(const ObLoadDatumRow *&datum_row);
private:
  common::ObArenaAllocator allocator_;
  blocksstable::ObStorageDatumUtils datum_utils_;
  ObLoadDatumRowCompare compare_;
  storage::ObExternalSort<ObLoadDatumRow, ObLoadDatumRowCompare> external_sort_;
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

// TODO: data sketch
// TODO: parallel optimize
class ObPartitionWriter
{
public:
  ObPartitionWriter() {}

  int init(const std::string& partition_directory) {
    partition_directory_ = partition_directory;
    return OB_SUCCESS;
  }

  static int64_t gen_key(int64_t pk) {
    return (pk - PK_MIN) / PK_SPAN;
  }

  int append_row(const ObLoadDatumRow *&datum_row) {
    // TODO: select pk via StorageDatumUtils
    int64_t key = gen_key(datum_row->datums_[0].get_int());

    char buf[1024];
    int64_t pos = 0;
    int ret = datum_row->serialize(buf, 1024, pos);

    partition_stream(key)->write(buf, pos);
    return ret;
  }

  void close() {
    for (auto &pair: streams_) {
      pair.second->close();
    }
  }

private:
  std::ofstream *partition_stream(int64_t key) {
    std::string file_path = partition_directory_ + "/" + std::to_string(key);
    auto iter = streams_.find(file_path);
    if (iter != streams_.end()) {
      return iter->second;
    }

    auto stream = new std::ofstream(file_path, std::ios::app);
    streams_[file_path] = stream;
    return stream;
  }

  std::string partition_directory_;
  std::map<std::string, std::ofstream *> streams_;
};

// TODO: parallel optimize
class ObPartitionReader
{
public:
  ObPartitionReader() {}

  // read partitions includes [range_min, range_max]
  int init(std::string partition_directory, int64_t range_min, int64_t range_max) {
    partition_directory_ = partition_directory;
    range_min_ = range_min;
    range_max_ = range_max;
    next_file_id_ = range_min_;

    int ret = OB_SUCCESS;
    if (OB_FAIL(buffer_.create(FILE_BUFFER_SIZE))) {
      LOG_WARN("fail to create buffer during partition reader init", KR(ret));
    } else if (OB_FAIL(open_next_partition())) {
      LOG_WARN("fail to open next partition", KR(ret));
    } else if (OB_FAIL(read_next_buffer())) {
      LOG_WARN("fail to read next buffer", KR(ret));
    }
    return ret;
  }

  int read(ObLoadDatumRow *&datum_row) {
    // TODO: select pk via StorageDatumUtils
    int ret;
    int64_t pos = 0;
    auto begin = buffer_.begin();
    auto end = buffer_.end();
    datum_row = new ObLoadDatumRow();
    if (OB_SUCC(datum_row->deserialize(begin, end - begin, pos))) {
      buffer_.consume(pos);
      return ret;
    }
    delete datum_row;
    datum_row = nullptr;

    bool file_consumed = false;
    if (OB_FAIL(read_next_buffer())) {
      if (ret == OB_ITER_END) {
        file_consumed = true;
      } else {
        LOG_WARN("fail to read next buffer with unexpected error", KR(ret));
      }
    }

    if (file_consumed) {
      if (OB_FAIL(open_next_partition())) {
        LOG_WARN("fail to open next partition", KR(ret));
        return OB_ITER_END;
      // } else if (OB_FAIL(read_next_buffer())) {
      //   LOG_WARN("fail to read next buffer", KR(ret));
      }
      return OB_EMPTY_RANGE;
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
          // this file has been consumed, and the next one will be read instead
          ret = OB_ITER_END;
        }
      }
    } else if (OB_UNLIKELY(buffer_.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected empty buffer", KR(ret));
    }
    return ret;
  }

  int open_next_partition() {
    int ret = OB_SUCCESS;
    int i = next_file_id_;
    for (; i <= range_max_; i++) {
      std::string file_path = partition_directory_ + "/" + std::to_string(i);
      ObString path{file_path.c_str()};

      file_reader_.close();
      if (OB_SUCC(file_reader_.open(path))) break;
      if (ret == OB_FILE_NOT_EXIST) continue;
      LOG_WARN("fail to open file during open next partition", KR(ret));
    }

    if (i > range_max_) {
      next_file_id_ = range_max_ + 1;
      return OB_ITER_END;
    }
    buffer_.reuse();
    next_file_id_ = i+1;
    LOG_INFO("open partition file successfully", K(i));
    return ret;
  }

private:
  std::string partition_directory_;
  int64_t range_min_;
  int64_t range_max_;
  int64_t next_file_id_;
  ObLoadSequentialFileReader file_reader_;
  ObLoadDataBuffer buffer_;
};

class ObRangePartitionSplitter
{
public:

  ObRangePartitionSplitter() {}

  int init(ObLoadDataStmt &load_stmt, std::string partition_directory) {
    int ret = OB_SUCCESS;
    remove_recursive(partition_directory.c_str());
    mkdir(PARTITION_DIR, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    mkdir(partition_directory.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);

    const ObLoadArgument &load_args = load_stmt.get_load_arguments();
    const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list =
      load_stmt.get_field_or_var_list();
    const uint64_t tenant_id = load_args.tenant_id_;
    const uint64_t table_id = load_args.table_id_;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id,
                                                                                    schema_guard))) {
      LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", KR(ret), K(tenant_id), K(table_id));
    } else if (OB_UNLIKELY(table_schema->is_heap_table())) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support heap table", KR(ret));
    }
    // init partition_writer_
    else if (OB_FAIL(partition_writer_.init(partition_directory))) {
      LOG_WARN("fail to init partition writer", KR(ret));
    }
    // init csv_parser_
    else if (OB_FAIL(csv_parser_.init(load_stmt.get_data_struct_in_file(), field_or_var_list.count(),
                                      load_args.file_cs_type_))) {
      LOG_WARN("fail to init csv parser", KR(ret));
    }
    // init file_reader_
    else if (OB_FAIL(file_reader_.open(load_args.full_file_path_))) {
      LOG_WARN("fail to open file", KR(ret), K(load_args.full_file_path_));
    }
    // init buffer_
    else if (OB_FAIL(buffer_.create(FILE_BUFFER_SIZE))) {
      LOG_WARN("fail to create buffer", KR(ret));
    }
    // init row_caster_
    else if (OB_FAIL(row_caster_.init(table_schema, field_or_var_list))) {
      LOG_WARN("fail to init row caster", KR(ret));
    }
    return ret;
  }

  int split() {
    int ret = OB_SUCCESS;
    const ObNewRow *new_row = nullptr;
    const ObLoadDatumRow *datum_row = nullptr;

    while (OB_SUCC(ret)) {
      if (OB_FAIL(buffer_.squash())) {
        LOG_WARN("fail to squash buffer", KR(ret));
      } else if (OB_FAIL(file_reader_.read_next_buffer(buffer_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to read next buffer", KR(ret));
        } else {
          if (OB_UNLIKELY(!buffer_.empty())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected incomplete data", KR(ret));
          }
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_UNLIKELY(buffer_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty buffer", KR(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(csv_parser_.get_next_row(buffer_, new_row))) {
            // TODO: faster parser
            if (OB_UNLIKELY(OB_ITER_END != ret)) {
              LOG_WARN("fail to get next row", KR(ret));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(row_caster_.get_casted_row(*new_row, datum_row))) {
            // Cost: 19.37%
            LOG_WARN("fail to cast row", KR(ret));
          } else if (OB_FAIL(partition_writer_.append_row(datum_row))) {
            LOG_WARN("fail to write partition", KR(ret));
          }
        }
      }
    }

    partition_writer_.close();
    return ret;
  }

private:
  static int remove_recursive(const char *const path) {
    DIR *const directory = opendir(path);
    if (directory) {
      struct dirent *entry;
      while ((entry = readdir(directory))) {
        if (!strcmp(".", entry->d_name) || !strcmp("..", entry->d_name)) {
          continue;
        }
        char filename[strlen(path) + strlen(entry->d_name) + 2];
        sprintf(filename, "%s/%s", path, entry->d_name);
        int (*const remove_func)(const char*) = entry->d_type == DT_DIR ? remove_recursive : remove;
        LOG_INFO("delete entry", K(entry->d_name));
        if (remove_func(entry->d_name)) {
          closedir(directory);
          return -1;
        }
      }
      if (closedir(directory)) {
        return -1;
      }
    }
    return remove(path);
  }

  ObLoadCSVPaser csv_parser_;
  ObLoadSequentialFileReader file_reader_;
  ObLoadDataBuffer buffer_;
  ObLoadRowCaster row_caster_;
  ObPartitionWriter partition_writer_;
};

class ObLoadSort
{
public:
  ObLoadSort() {}

  int init(const share::schema::ObTableSchema *table_schema,
          int64_t mem_size,
          int64_t file_buf_size) {
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

  int reuse() {
    datum_rows_.clear();
    pos_ = 0;
    return OB_SUCCESS;
  }

  int append_row(ObLoadDatumRow *datum_row) {
    if (datum_row == nullptr) {
      LOG_WARN("received null datum row");
      return OB_ERR_UNEXPECTED;
    }

    datum_rows_.push_back(datum_row);
    int64_t pk1 = datum_row->datums_[0].get_int();
    int64_t pk2 = datum_row->datums_[1].get_int();
    LOG_INFO("sort append element: ", K(pk1), K(pk2), K((long)(datum_row))); //, K(*datum_row));
    return OB_SUCCESS;
  }

  int close() {
    std::sort(datum_rows_.begin(), datum_rows_.end(), compare_);
    for (auto &datum_row : datum_rows_) {
      int64_t pk1 = datum_row->datums_[0].get_int();
      int64_t pk2 = datum_row->datums_[1].get_int();
      LOG_INFO("sort element: ", K(pk1), K(pk2), K((long)(datum_row))); //, K(*datum_row));
    }
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
  std::vector<ObLoadDatumRow *> datum_rows_;
  int pos_;
  bool is_closed_;
  bool is_inited_;
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
    ObRangePartitionSplitter partition_splitter_;
    ObPartitionReader partition_reader_;
    ObLoadCSVPaser csv_parser_;
    ObLoadSequentialFileReader file_reader_;
    ObLoadDataBuffer buffer_;
    ObLoadRowCaster row_caster_;
    ObLoadSort sort_;
    ObLoadSSTableWriter sstable_writer_;
};
} // namespace sql
} // namespace oceanbase