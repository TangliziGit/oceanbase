// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/cmd/ob_load_data_direct.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_thread_pool.h"

namespace oceanbase
{
namespace sql
{
using namespace blocksstable;
using namespace common;
using namespace lib;
using namespace observer;
using namespace share;
using namespace share::schema;

/**
 * ObLoadDataBuffer
 */

ObLoadDataBuffer::ObLoadDataBuffer()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), data_(nullptr), begin_pos_(0), end_pos_(0), capacity_(0)
{
}

ObLoadDataBuffer::~ObLoadDataBuffer()
{
  reset();
}

void ObLoadDataBuffer::reuse()
{
  begin_pos_ = 0;
  end_pos_ = 0;
}

void ObLoadDataBuffer::reset()
{
  allocator_.reset();
  data_ = nullptr;
  begin_pos_ = 0;
  end_pos_ = 0;
  capacity_ = 0;
}

int ObLoadFileDataBuffer::create(int64_t capacity, int32_t task_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != data_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataBuffer init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else if (OB_UNLIKELY(task_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(task_id));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(data_ = static_cast<char *>(allocator_.alloc(capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(capacity));
    } else {
      file_begin_pos_ = TASK_SIZE * task_id;
      file_end_pos_ = TASK_SIZE * (task_id + 1);
      capacity_ = capacity;
    }
    if (task_id != 0) {
      set_step(true);
    }
  }
  return ret;
}

int ObLoadDataBuffer::create(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != data_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataBuffer init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    if (OB_ISNULL(data_ = static_cast<char *>(allocator_.alloc(capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(capacity));
    } else {
      capacity_ = capacity;
    }
  }
  return ret;
}

int ObLoadDataBuffer::squash()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataBuffer not init", KR(ret), KP(this));
  } else {
    const int64_t data_size = get_data_size();
    if (data_size > 0) {
      MEMMOVE(data_, data_ + begin_pos_, data_size);
    }
    begin_pos_ = 0;
    end_pos_ = data_size;
  }
  return ret;
}

/**
 * ObLoadSequentialFileReader
 */

ObLoadSequentialFileReader::ObLoadSequentialFileReader()
  : offset_(0), is_read_end_(false)
{
}

ObLoadSequentialFileReader::~ObLoadSequentialFileReader()
{
}

int ObLoadSequentialFileReader::open(const ObString &filepath)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(file_reader_.open(filepath, false))) {
    LOG_WARN("fail to open file", KR(ret), K(filepath));
  }
  return ret;
}

int ObLoadSequentialFileReader::read_next_buffer(ObLoadDataBuffer &buffer, int64_t offset)
{
  offset = (offset > 0)? offset: offset_;
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_reader_.is_opened())) {
    ret = OB_FILE_NOT_OPENED;
    LOG_WARN("file not opened", KR(ret));
  } else if (OB_LIKELY(buffer.get_remain_size() > 0)) {
    const int64_t buffer_remain_size = buffer.get_remain_size();
    int64_t read_size = 0;
    if (OB_FAIL(file_reader_.pread(buffer.end(), buffer_remain_size, offset, read_size))) {
      LOG_WARN("fail to do pread", KR(ret));
    } else if (read_size == 0) {
      // is_read_end_ = true;
      ret = OB_ITER_END;
    } else {
      offset_ += read_size;
      buffer.produce(read_size);
    }
  }
  return ret;
}

/**
 * ObLoadCSVPaser
 */

ObLoadCSVPaser::ObLoadCSVPaser()
  : allocator_(ObModIds::OB_SQL_LOAD_DATA), collation_type_(CS_TYPE_INVALID), is_inited_(false)
{
}

ObLoadCSVPaser::~ObLoadCSVPaser()
{
  reset();
}

void ObLoadCSVPaser::reset()
{
  allocator_.reset();
  collation_type_ = CS_TYPE_INVALID;
  row_.reset();
  err_records_.reset();
  is_inited_ = false;
}

int ObLoadCSVPaser::init(const ObDataInFileStruct &format, int64_t column_count,
                         ObCollationType collation_type)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadCSVPaser init twice", KR(ret), KP(this));
  } else if (OB_FAIL(csv_parser_.init(format, column_count, collation_type))) {
    LOG_WARN("fail to init csv parser", KR(ret));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    ObObj *objs = nullptr;
    if (OB_ISNULL(objs = static_cast<ObObj *>(allocator_.alloc(sizeof(ObObj) * column_count)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      new (objs) ObObj[column_count];
      row_.cells_ = objs;
      row_.count_ = column_count;
      collation_type_ = collation_type;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadCSVPaser::get_next_row(ObLoadFileDataBuffer &buffer, const ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (buffer.empty()) {
    ret = OB_ITER_END;
    LOG_WARN("fail to get next row because buffer is empty",
             KR(ret), K(buffer.get_data_size()), K(buffer.get_file_pos()), K(buffer.get_offset()));
  } else {
    const char *str = buffer.begin();
    const char *end = buffer.end();
    int64_t nrows = 1;
    if (OB_FAIL(csv_parser_.scan(str, end, nrows, nullptr, nullptr, unused_row_handler_,
                                 err_records_, false))) {
      LOG_WARN("fail to scan buffer", KR(ret));
    } else if (buffer.should_step()) {
      ret = OB_NEED_RETRY;
      err_records_.reuse();
      buffer.set_step(false);
      buffer.consume(str - buffer.begin());
      // LOG_INFO("task start offset.", K(buffer.get))
    } else if (OB_UNLIKELY(!err_records_.empty())) {
      ret = err_records_.at(0).err_code;
      LOG_WARN("fail to parse line", KR(ret));
    } else if (0 == nrows) {
      ret = OB_ITER_END;
      // LOG_WARN("end of file", KR(ret));
    } else if (buffer.is_compete()) {
      ret = OB_TASK_FINISH;
      LOG_INFO("TASK FINISH .", K(buffer.get_file_pos()));    
    } else {
      buffer.consume(str - buffer.begin());
      const ObIArray<ObCSVGeneralParser::FieldValue> &field_values_in_file =
        csv_parser_.get_fields_per_line();
      for (int64_t i = 0; i < row_.count_; ++i) {
        const ObCSVGeneralParser::FieldValue &str_v = field_values_in_file.at(i);
        ObObj &obj = row_.cells_[i];
        if (str_v.is_null_) {
          obj.set_null();
        } else {
          obj.set_string(ObVarcharType, ObString(str_v.len_, str_v.ptr_));
          obj.set_collation_type(collation_type_);
        }
      }
      row = &row_;
    }
  }
  return ret;
}

/**
 * ObLoadDatumRow
 */

ObLoadDatumRow::ObLoadDatumRow()
  : capacity_(0), count_(0), rowkey_column_num_(0),extra_column_num_(0),datums_(nullptr)
{
}
ObLoadDatumRow::ObLoadDatumRow(int64_t rowkey_column_num, int64_t extra_column_num)
  : capacity_(0), count_(0), rowkey_column_num_(rowkey_column_num),extra_column_num_(extra_column_num),datums_(nullptr)
{
}

ObLoadDatumRow::~ObLoadDatumRow()
{
}

void ObLoadDatumRow::reset()
{
  // allocator_.reset();
  capacity_ = 0;
  count_ = 0;
  datums_ = nullptr;
}

int ObLoadDatumRow::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(capacity));
  } else {
    reset();
    // allocator_.set_tenant_id(MTL_ID());
    auto &allocator = ObLoadDatumRowAllocator::get_allocator();
    if (OB_ISNULL(datums_ = static_cast<ObStorageDatum *>(
                    allocator.alloc(sizeof(ObStorageDatum) * capacity)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else {
      new (datums_) ObStorageDatum[capacity];
      capacity_ = capacity;
      count_ = capacity;
    }
  }
  return ret;
}

int64_t ObLoadDatumRow::get_deep_copy_size() const
{
  int64_t size = 0;
  size += sizeof(ObStorageDatum) * count_;
  for (int64_t i = 0; i < count_; ++i) {
    size += datums_[i].get_deep_copy_size();
  }
  return size;
}

int ObLoadDatumRow::deep_copy(const ObLoadDatumRow &src, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!src.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(src));
  } else {
    reset();
    ObStorageDatum *datums = nullptr;
    const int64_t datum_cnt = src.count_;
    datums = new (buf + pos) ObStorageDatum[datum_cnt];
    pos += sizeof(ObStorageDatum) * datum_cnt;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_cnt; ++i) {
      if (OB_FAIL(datums[i].deep_copy(src.datums_[i], buf, len, pos))) {
        LOG_WARN("fail to deep copy storage datum", KR(ret), K(src.datums_[i]));
      }
    }
    if (OB_SUCC(ret)) {
      capacity_ = datum_cnt;
      count_ = datum_cnt;
      datums_ = datums;
    }
  }
  return ret;
}

DEF_TO_STRING(ObLoadDatumRow)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(capacity), K_(count));
  if (nullptr != datums_) {
    J_ARRAY_START();
    for (int64_t i = 0; i < count_; ++i) {
      databuff_printf(buf, buf_len, pos, "col_id=%ld:", i);
      pos += datums_[i].storage_to_string(buf + pos, buf_len - pos);
      databuff_printf(buf, buf_len, pos, ",");
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

OB_DEF_SERIALIZE(ObLoadDatumRow)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE_ARRAY(datums_, count_);
  return ret;
}

OB_DEF_DESERIALIZE(ObLoadDatumRow)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;
  OB_UNIS_DECODE(count);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count", K(count));
    } else if (count > capacity_ && OB_FAIL(init(count + extra_column_num_))) {
      LOG_WARN("fail to init", KR(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < (count); ++i) {       
        if (i < rowkey_column_num_) {
          OB_UNIS_DECODE(datums_[i]);
        } else {
          OB_UNIS_DECODE(datums_[i + extra_column_num_]);
        }                             
      }
      datums_[rowkey_column_num_].set_int(-1); // fill trans_version
      datums_[rowkey_column_num_ + 1].set_int(0); // fill sql_no
      count_ = count + extra_column_num_;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObLoadDatumRow)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(datums_, count_);
  return len;
}

/**
 * ObLoadDatumRowCompare
 */

ObLoadDatumRowCompare::ObLoadDatumRowCompare()
  : result_code_(OB_SUCCESS), rowkey_column_num_(0), datum_utils_(nullptr), is_inited_(false)
{
}

ObLoadDatumRowCompare::~ObLoadDatumRowCompare()
{
}

int ObLoadDatumRowCompare::init(int64_t rowkey_column_num, const ObStorageDatumUtils *datum_utils)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDatumRowCompare init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(rowkey_column_num <= 0 || nullptr == datum_utils)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(rowkey_column_num), KP(datum_utils));
  } else {
    rowkey_column_num_ = rowkey_column_num;
    datum_utils_ = datum_utils;
    is_inited_ = true;
  }
  return ret;
}

bool ObLoadDatumRowCompare::operator()(const ObLoadDatumRow *lhs, const ObLoadDatumRow *rhs)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDirectLoadDatumRowCompare not init", KR(ret), KP(this));
  } else if (OB_ISNULL(lhs) || OB_ISNULL(rhs) ||
             OB_UNLIKELY(!lhs->is_valid() || !rhs->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(lhs), KPC(rhs));
  } else if (OB_UNLIKELY(lhs->count_ < rowkey_column_num_ || rhs->count_ < rowkey_column_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", KR(ret), KPC(lhs), KPC(rhs), K_(rowkey_column_num));
  } else {
    if (OB_FAIL(lhs_rowkey_.assign(lhs->datums_, rowkey_column_num_))) {
      LOG_WARN("fail to assign datum rowkey", KR(ret), K(lhs), K_(rowkey_column_num));
    } else if (OB_FAIL(rhs_rowkey_.assign(rhs->datums_, rowkey_column_num_))) {
      LOG_WARN("fail to assign datum rowkey", KR(ret), K(rhs), K_(rowkey_column_num));
    } else if (OB_FAIL(lhs_rowkey_.compare(rhs_rowkey_, *datum_utils_, cmp_ret))) {
      LOG_WARN("fail to compare rowkey", KR(ret), K(rhs_rowkey_), K(rhs_rowkey_), KP(datum_utils_));
    }
  }
  if (OB_FAIL(ret)) {
    result_code_ = ret;
  }
  return cmp_ret < 0;
}

/**
 * ObLoadRowCaster
 */

ObLoadRowCaster::ObLoadRowCaster()
  : column_count_(0),
    collation_type_(CS_TYPE_INVALID),
    cast_allocator_(ObModIds::OB_SQL_LOAD_DATA),
    is_inited_(false)
{
}

ObLoadRowCaster::~ObLoadRowCaster()
{
}

int ObLoadRowCaster::init(const ObTableSchema *table_schema,
                          const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadRowCaster init twice", KR(ret));
  } else if (OB_UNLIKELY(nullptr == table_schema || field_or_var_list.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema), K(field_or_var_list));
  } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(MTL_ID(), tz_info_.get_tz_map_wrap()))) {
    LOG_WARN("fail to get tenant time zone", KR(ret));
  } else if (OB_FAIL(init_column_schemas_and_idxs(table_schema, field_or_var_list))) {
    LOG_WARN("fail to init column schemas and idxs", KR(ret));
  } else if (OB_FAIL(datum_row_.init(table_schema->get_column_count()))) {
    LOG_WARN("fail to init datum row", KR(ret));
  } else {
    ObDataTypeCastParams cast_params(&tz_info_);
    cast_params_ = cast_params;
    column_count_ = table_schema->get_column_count();
    collation_type_ = table_schema->get_collation_type();
    cast_allocator_.set_tenant_id(MTL_ID());
    is_inited_ = true;
  }
  return ret;
}

int ObLoadRowCaster::init_column_schemas_and_idxs(
  const ObTableSchema *table_schema,
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 64> column_descs;
  if (OB_FAIL(table_schema->get_column_ids(column_descs))) {
    LOG_WARN("fail to get column descs", KR(ret), KPC(table_schema));
  } else {
    bool found_column = true;
    for (int64_t i = 0; OB_SUCC(ret) && OB_LIKELY(found_column) && i < column_descs.count(); ++i) {
      const ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema", KR(ret), K(col_desc));
      } else if (OB_UNLIKELY(col_schema->is_hidden())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected hidden column", KR(ret), K(i), KPC(col_schema));
      } else if (OB_FAIL(column_schemas_.push_back(col_schema))) {
        LOG_WARN("fail to push back column schema", KR(ret));
      } else {
        found_column = false;
      }
      // find column in source data columns
      for (int64_t j = 0; OB_SUCC(ret) && OB_LIKELY(!found_column) && j < field_or_var_list.count();
           ++j) {
        const ObLoadDataStmt::FieldOrVarStruct &field_or_var_struct = field_or_var_list.at(j);
        if (col_desc.col_id_ == field_or_var_struct.column_id_) {
          found_column = true;
          if (OB_FAIL(column_idxs_.push_back(j))) {
            LOG_WARN("fail to push back column idx", KR(ret), K(column_idxs_), K(i), K(col_desc),
                     K(j), K(field_or_var_struct));
          }
        }
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(!found_column)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not supported incomplete column data", KR(ret), K(column_idxs_), K(column_descs),
               K(field_or_var_list));
    }
  }
  return ret;
}

int ObLoadRowCaster::get_casted_row(const ObNewRow &new_row, const ObLoadDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadRowCaster not init", KR(ret));
  } else {
    const int64_t extra_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    cast_allocator_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_idxs_.count(); ++i) {
      int64_t column_idx = column_idxs_.at(i);
      if (OB_UNLIKELY(column_idx < 0 || column_idx >= new_row.count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected column idx", KR(ret), K(column_idx), K(new_row.count_));
      } else {
        const ObColumnSchemaV2 *column_schema = column_schemas_.at(i);
        const ObObj &src_obj = new_row.cells_[column_idx];
        ObStorageDatum &dest_datum = datum_row_.datums_[i];
        if (OB_FAIL(cast_obj_to_datum(column_schema, src_obj, dest_datum))) {
          LOG_WARN("fail to cast obj to datum", KR(ret), K(src_obj));
        }
      }
    }
    if (OB_SUCC(ret)) {
      datum_row = &datum_row_;
    }
  }
  return ret;
}

int ObLoadRowCaster::cast_obj_to_datum(const ObColumnSchemaV2 *column_schema, const ObObj &obj,
                                       ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  // ObDataTypeCastParams cast_params(&tz_info_);
  ObCastCtx cast_ctx(&cast_allocator_, &cast_params_, CM_NONE, collation_type_);
  const ObObjType expect_type = column_schema->get_meta_type().get_type();
  ObObj casted_obj;
  if (obj.is_null()) {
    casted_obj.set_null();
  } else if (is_oracle_mode() && (obj.is_null_oracle() || 0 == obj.get_val_len())) {
    casted_obj.set_null();
  } else if (is_mysql_mode() && 0 == obj.get_val_len() && !ob_is_string_tc(expect_type)) {
    ObObj zero_obj;
    zero_obj.set_int(0);
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, zero_obj, casted_obj))) {
      LOG_WARN("fail to do to type", KR(ret), K(zero_obj), K(expect_type));
    }
  } else {
    if (OB_FAIL(ObObjCaster::to_type(expect_type, cast_ctx, obj, casted_obj))) {
      LOG_WARN("fail to do to type", KR(ret), K(obj), K(expect_type));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum.from_obj_enhance(casted_obj))) {
      LOG_WARN("fail to from obj enhance", KR(ret), K(casted_obj));
    }
  }
  return ret;
}

/**
 * ObLoadSSTableWriter
 */

ObLoadSSTableWriter::ObLoadSSTableWriter()
  : rowkey_column_num_(0),
    extra_rowkey_column_num_(0),
    column_count_(0),
    is_closed_(false),
    is_inited_(false)
{
}

ObLoadSSTableWriter::~ObLoadSSTableWriter()
{
}

int ObLoadSSTableWriter::init(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadSSTableWriter init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(table_schema));
  } else {
    tablet_id_ = table_schema->get_tablet_id();
    rowkey_column_num_ = table_schema->get_rowkey_column_num();
    extra_rowkey_column_num_ = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    column_count_ = table_schema->get_column_count();
    ObLocationService *location_service = nullptr;
    bool is_cache_hit = false;
    ObLSService *ls_service = nullptr;
    ObLS *ls = nullptr;
    if (OB_ISNULL(location_service = GCTX.location_service_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("location service is null", KR(ret), KP(location_service));
    } else if (OB_FAIL(
                 location_service->get(MTL_ID(), tablet_id_, INT64_MAX, is_cache_hit, ls_id_))) {
      LOG_WARN("fail to get ls id", KR(ret), K(tablet_id_));
    } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
      ret = OB_ERR_SYS;
      LOG_ERROR("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle_, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls_id_));
    } else if (OB_ISNULL(ls = ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls should not be null", KR(ret));
    } else if (OB_FAIL(ls->get_tablet(tablet_id_, tablet_handle_))) {
      LOG_WARN("fail to get tablet handle", KR(ret), K(tablet_id_));
    } else if (OB_FAIL(init_sstable_index_builder(table_schema))) {
      LOG_WARN("fail to init sstable index builder", KR(ret));
    } else if (OB_FAIL(data_store_desc_.init(*table_schema, ls_id_, tablet_id_, MAJOR_MERGE, 1))) {
      LOG_WARN("fail to init data_store_desc", KR(ret), K(tablet_id_));
    } else {
      for (int i = 0; i < N_CPU; i++) {
        if (OB_FAIL(datum_rows_[i].init(column_count_ + extra_rowkey_column_num_))) {
          LOG_WARN("fail to init datum row", KR(ret));
          break;
        }
        datum_rows_[i].row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        datum_rows_[i].mvcc_row_flag_.set_last_multi_version_row(true);
        datum_rows_[i].storage_datums_[rowkey_column_num_].set_int(-1); // fill trans_version
        datum_rows_[i].storage_datums_[rowkey_column_num_ + 1].set_int(0); // fill sql_no
      }
      table_key_.table_type_ = ObITable::MAJOR_SSTABLE;
      table_key_.tablet_id_ = tablet_id_;
      table_key_.log_ts_range_.start_log_ts_ = 0;
      table_key_.log_ts_range_.end_log_ts_ = ObTimeUtil::current_time_ns();
      data_store_desc_.sstable_index_builder_ = &sstable_index_builder_;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadSSTableWriter::init_sstable_index_builder(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObDataStoreDesc data_desc;
  if (OB_FAIL(data_desc.init(*table_schema, ls_id_, tablet_id_, MAJOR_MERGE, 1L))) {
    LOG_WARN("fail to init data desc", KR(ret));
  } else {
    data_desc.row_column_count_ = data_desc.rowkey_column_count_ + 1;
    data_desc.need_prebuild_bloomfilter_ = false;
    data_desc.col_desc_array_.reset();
    if (OB_FAIL(data_desc.col_desc_array_.init(data_desc.row_column_count_))) {
      LOG_WARN("fail to reserve column desc array", KR(ret));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(data_desc.col_desc_array_))) {
      LOG_WARN("fail to get rowkey column ids", KR(ret));
    } else if (OB_FAIL(
                 ObMultiVersionRowkeyHelpper::add_extra_rowkey_cols(data_desc.col_desc_array_))) {
      LOG_WARN("fail to add extra rowkey cols", KR(ret));
    } else {
      ObObjMeta meta;
      meta.set_varchar();
      meta.set_collation_type(CS_TYPE_BINARY);
      ObColDesc col;
      col.col_id_ = static_cast<uint64_t>(data_desc.row_column_count_ + OB_APP_MIN_COLUMN_ID);
      col.col_type_ = meta;
      col.col_order_ = DESC;
      if (OB_FAIL(data_desc.col_desc_array_.push_back(col))) {
        LOG_WARN("fail to push back last col for index", KR(ret), K(col));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(sstable_index_builder_.init(data_desc))) {
      LOG_WARN("fail to init index builder", KR(ret), K(data_desc));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::reuse_macro_block_writer(const int index,const int64_t task_id, const share::schema::ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  macro_block_writers_[index].reset();
  if (OB_SUCC(ret)) {
    ObMacroDataSeq data_seq;
    data_seq.set_parallel_degree(task_id * 100000000);
    if (OB_FAIL(macro_block_writers_[index].open(data_store_desc_, data_seq))) {
      LOG_WARN("fail to init macro block writer", KR(ret), K(data_store_desc_), K(data_seq));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::close_macro_block_writer(const int index) 
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(macro_block_writers_[index].close())) {
    LOG_WARN("fail to close macro block writer", KR(ret));
  } 
  return ret;
}

int ObLoadSSTableWriter::append_row(const int index, const ObLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed external sort", KR(ret));
  } else if (OB_UNLIKELY(!datum_row.is_valid() || datum_row.count_ != column_count_ + extra_rowkey_column_num_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(datum_row), K(column_count_));
  } else {
    /*for (int64_t i = 0; i < column_count_; ++i) {
      if (i < rowkey_column_num_) {
        datum_rows_[index].storage_datums_[i] = datum_row.datums_[i];
      } else {
        datum_rows_[index].storage_datums_[i + extra_rowkey_column_num_] = datum_row.datums_[i];
      }
    }*/
    datum_rows_[index].storage_datums_ = datum_row.datums_;
    if (OB_FAIL(macro_block_writers_[index].append_row(datum_rows_[index]))) {
      int64_t pk1 = datum_row.datums_[0].get_int();
      int64_t pk2 = datum_row.datums_[1].get_int();
      LOG_WARN("fail to append row", KR(ret), K(pk1), K(pk2), K(index));
    }
  }
  return ret;
}

int ObLoadSSTableWriter::create_sstable()
{
  int ret = OB_SUCCESS;
  ObTableHandleV2 table_handle;
  SMART_VAR(ObSSTableMergeRes, merge_res)
  {
    const ObStorageSchema &storage_schema = tablet_handle_.get_obj()->get_storage_schema();
    int64_t column_count = 0;
    if (OB_FAIL(storage_schema.get_stored_column_count_in_sstable(column_count))) {
      LOG_WARN("fail to get stored column count in sstable", KR(ret));
    } else if (OB_FAIL(sstable_index_builder_.close(column_count, merge_res))) {
      LOG_WARN("fail to close sstable index builder", KR(ret));
    } else {
      ObTabletCreateSSTableParam create_param;
      create_param.table_key_ = table_key_;
      create_param.table_mode_ = storage_schema.get_table_mode_struct();
      create_param.index_type_ = storage_schema.get_index_type();
      create_param.rowkey_column_cnt_ = storage_schema.get_rowkey_column_num() +
                                        ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
      create_param.schema_version_ = storage_schema.get_schema_version();
      create_param.create_snapshot_version_ = 0;
      ObSSTableMergeRes::fill_addr_and_data(merge_res.root_desc_, create_param.root_block_addr_,
                                            create_param.root_block_data_);
      ObSSTableMergeRes::fill_addr_and_data(merge_res.data_root_desc_,
                                            create_param.data_block_macro_meta_addr_,
                                            create_param.data_block_macro_meta_);
      create_param.root_row_store_type_ = merge_res.root_desc_.row_type_;
      create_param.data_index_tree_height_ = merge_res.root_desc_.height_;
      create_param.index_blocks_cnt_ = merge_res.index_blocks_cnt_;
      create_param.data_blocks_cnt_ = merge_res.data_blocks_cnt_;
      create_param.micro_block_cnt_ = merge_res.micro_block_cnt_;
      create_param.use_old_macro_block_count_ = merge_res.use_old_macro_block_count_;
      create_param.row_count_ = merge_res.row_count_;
      create_param.column_cnt_ = merge_res.data_column_cnt_;
      create_param.data_checksum_ = merge_res.data_checksum_;
      create_param.occupy_size_ = merge_res.occupy_size_;
      create_param.original_size_ = merge_res.original_size_;
      create_param.max_merged_trans_version_ = merge_res.max_merged_trans_version_;
      create_param.contain_uncommitted_row_ = merge_res.contain_uncommitted_row_;
      create_param.compressor_type_ = merge_res.compressor_type_;
      create_param.encrypt_id_ = merge_res.encrypt_id_;
      create_param.master_key_id_ = merge_res.master_key_id_;
      create_param.data_block_ids_ = merge_res.data_block_ids_;
      create_param.other_block_ids_ = merge_res.other_block_ids_;
      MEMCPY(create_param.encrypt_key_, merge_res.encrypt_key_,
             OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
      if (OB_FAIL(
            merge_res.fill_column_checksum(&storage_schema, create_param.column_checksums_))) {
        LOG_WARN("fail to fill column checksum for empty major", KR(ret), K(create_param));
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(create_param, table_handle))) {
        LOG_WARN("fail to create sstable", KR(ret), K(create_param));
      } else {
        const int64_t rebuild_seq = ls_handle_.get_ls()->get_rebuild_seq();
        ObTabletHandle new_tablet_handle;
        ObUpdateTableStoreParam table_store_param(table_handle,
                                                  tablet_handle_.get_obj()->get_snapshot_version(),
                                                  false, &storage_schema, rebuild_seq, true, true);
        if (OB_FAIL(ls_handle_.get_ls()->update_tablet_table_store(tablet_id_, table_store_param,
                                                                   new_tablet_handle))) {
          LOG_WARN("fail to update tablet table store", KR(ret), K(tablet_id_),
                   K(table_store_param));
        }
      }
    }
  }
  return ret;
}

int ObLoadSSTableWriter::close()
{
  int ret = OB_SUCCESS;
  for (int i = 0; i < N_CPU; i++) {
    macro_block_writers_[i].reset();
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadSSTableWriter not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected closed sstable writer", KR(ret));
  } else {
    ObSSTable *sstable = nullptr;
    if (OB_FAIL(create_sstable())) {
      LOG_WARN("fail to create sstable", KR(ret));
    } else {
      is_closed_ = true;
    }
  }
  for (int i = 0; i < N_CPU; i++) {
    macro_block_writers_[i].reset();
  }
  return ret;
}

int ObLoadDataSplitThreadPool::init(ObLoadDataStmt *load_stmt,
                                    const ObTableSchema *table_schema,
                                    std::string partition_directory)
{
  load_stmt_ = load_stmt;
  table_schema_ = table_schema;

  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt->get_load_arguments();
  if (OB_FAIL(file_reader_.open(load_args.full_file_path_))) {
    LOG_WARN("fail to open file", KR(ret), K(load_args.full_file_path_));
  } else if (OB_FAIL(partition_writer_.init(partition_directory))) {
    LOG_WARN("fail to init partition writer", KR(ret));
  }
  return ret;
}

void ObLoadDataSplitThreadPool::run1()
{
  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  ObTenantBase *tenant_base = MTL_CTX();
  Worker::CompatMode mode = ((ObTenant*)tenant_base)->get_compat_mode();
  Worker::set_compatibility_mode(mode);

  const ObLoadArgument &load_args = load_stmt_->get_load_arguments();
  const auto &field_or_var_list = load_stmt_->get_field_or_var_list();
  const int64_t thread_id = (const int64_t)(this->get_thread_idx());
  auto &datum_row_allocator = ObLoadDatumRowAllocator::get_allocator();
  datum_row_allocator.set_tenant_id(MTL_ID());
  LOG_INFO("data split start" , K(thread_id), K(MTL_ID()));

  int ret = OB_SUCCESS;
  int all_size = 0;
  while (ret_.load() == OB_SUCCESS) {
    int32_t task_id = get_task();
    ObLoadCSVPaser csv_parser;
    ObLoadFileDataBuffer buffer;
    ObLoadRowCaster row_caster;

    // init buffer
    if (OB_FAIL(buffer.create(FILE_DATA_BUFFER_SIZE, task_id))) {
      LOG_WARN("fail to create buffer", KR(ret));
    }
    // TODO: unnecessary initialization
    // init csv_parser
    else if (OB_FAIL(csv_parser.init(load_stmt_->get_data_struct_in_file(), field_or_var_list.count(),
                                  load_args.file_cs_type_))) {
      LOG_WARN("fail to init csv parser", KR(ret));
    }
    // TODO: unnecessary initialization
    // init row_caster_
    else if (OB_FAIL(row_caster.init(table_schema_, field_or_var_list))) {
      LOG_WARN("fail to init row caster", KR(ret));
    }
    else {
      const ObNewRow *new_row = nullptr;
      const ObLoadDatumRow *datum_row = nullptr;
      int block_id = 0;
      int task_num = 0;
      LOG_INFO("TASK START!", K(task_id));
      while (OB_SUCC(ret)) {
        if (OB_FAIL(buffer.squash())) {
          LOG_WARN("fail to squash buffer", KR(ret));
        } else if (OB_FAIL(file_reader_.read_next_buffer(buffer, buffer.get_offset()))) {
          if (OB_UNLIKELY(OB_ITER_END == ret)) {
            if (OB_UNLIKELY(!buffer.empty())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected incomplate data", KR(ret));
            }
            break;
          } else {
            LOG_WARN("fail to read next buffer", KR(ret));
          }
        } else if (OB_UNLIKELY(buffer.empty())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected empty buffer", KR(ret));
        } else {
          //LOG_INFO("TASK block", K(task_id), K(block_id), K(ret));
          while (OB_SUCC(ret)) {
            if (OB_FAIL(csv_parser.get_next_row(buffer, new_row))) {
              if (OB_UNLIKELY(OB_ITER_END == ret)) {
                //LOG_WARN("fail to get next row", KR(ret));
              } else if (OB_UNLIKELY(OB_TASK_FINISH == ret)) {
                //LOG_INFO("TASK : HAS BEEN FINISHED !!!", K(task_id));
              } else if (OB_UNLIKELY(OB_NEED_RETRY == ret)) {
                ret = OB_SUCCESS;
                continue;
              }
            } else if (OB_FAIL(row_caster.get_casted_row(*new_row, datum_row))) {
              LOG_WARN("fail to cast row", KR(ret));
            } else if (OB_FAIL(partition_writer_.append_row(datum_row))) {
              LOG_WARN("fail to append row", KR(ret));
            } else {
              task_num++;
            }
          }
          if (OB_UNLIKELY(OB_ITER_END == ret)) {
            ret = OB_SUCCESS;
          }
          block_id++;
        }

        //LOG_INFO("TASK block",K(thread_id), K(task_id), K(task_num), K(block_id), K(ret));
      }

      all_size += task_num;
      // LOG_INFO("TASK tt block",K(thread_id), K(task_id), K(task_num), K(block_id), K(ret));
      
      if (OB_UNLIKELY(ret == OB_ITER_END)) {
        ret = OB_SUCCESS;
        break;
      } else if (OB_UNLIKELY(ret == OB_TASK_FINISH)) {
        ret = OB_SUCCESS;
      } else {
        ret_.store(ret);
        break;
      }
    }

    datum_row_allocator.reset();
    if (OB_FAIL(ret)) {
      ret_.store(ret);
      LOG_WARN("loop err", KR(ret));
    }
  }

  datum_row_allocator.reset();
  if (OB_FAIL(ret)) {
    ret_.store(ret);
  }
  LOG_INFO("TASK all tt block",K(thread_id), K(all_size), K(ret));
}

void ObSStableWriterThreadPool::run1()
{
  ObTenantStatEstGuard stat_est_guard(MTL_ID());
  ObTenantBase *tenant_base = MTL_CTX();
  Worker::CompatMode mode = ((ObTenant*)tenant_base)->get_compat_mode();
  Worker::set_compatibility_mode(mode);
  // TODO
  const int64_t thread_id = (const int64_t)(this->get_thread_idx());
  auto &datum_row_allocator = ObLoadDatumRowAllocator::get_allocator();
  datum_row_allocator.set_tenant_id(MTL_ID());

  int ret = OB_SUCCESS;
  int64_t all_size = 0;
  while (OB_SUCC(ret_.load())) {
    int32_t task_id = get_task();
    if (task_id > max_id_) {
      if (OB_FAIL(sstable_writer_.reuse_macro_block_writer(thread_id, task_id, table_schema_))) {
        LOG_WARN("fail to reuse the sstable writer.", K(ret), K(thread_id), K(task_id));
      }
      break;
    }
    if (task_id == 1144) {
      LOG_INFO("-----");
    }
    LOG_INFO("load task start.", K(ret), K(thread_id), K(task_id));
    ObLoadDatumRow *datum_row = nullptr;
    ObLoadSort load_sort;
    ObPartitionReader partition_reader;
    if (OB_FAIL(sstable_writer_.reuse_macro_block_writer(thread_id, task_id, table_schema_))) {
      LOG_WARN("fail to reuse the sstable writer.", K(ret), K(thread_id), K(task_id));
    } else if (OB_FAIL(partition_reader.init(partition_directory_, task_id, table_schema_->get_column_count()))) {
      LOG_WARN("fail to init the partition reader.", K(ret), K(thread_id), K(task_id));
    } else if (OB_FAIL(load_sort.init(table_schema_))) {
      LOG_WARN("fail to init the sort.", K(ret), K(thread_id), K(task_id));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(partition_reader.read(datum_row))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to read the next row of partition reader.", K(ret), K(thread_id), K(task_id));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(load_sort.append_row(datum_row))) {
          LOG_WARN("fail to append row to the sort.", K(ret), K(thread_id), K(task_id));
        } else {
          if (task_id == 1144) {
            int64_t row = datum_row->datums_[0].get_int();
            if (row < 0) {
              LOG_INFO("ffffff",K(row));
            } else {
              LOG_INFO("jjjjjj",K(row));
            }
            
          }
          all_size++;
        }
      }
      LOG_INFO("TASK step 1",K(thread_id), K(task_id) ,K(all_size), K(ret));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(load_sort.close())) {
          LOG_WARN("fail to close the sort.", K(ret), K(thread_id), K(task_id));
        }
      }
      LOG_INFO("TASK step 2",K(thread_id), K(task_id) ,K(all_size), K(ret));
      while (OB_SUCC(ret)) {
        if (OB_FAIL(load_sort.get_next_row(datum_row))) {
          if (ret == OB_ITER_END) {
            // all rows in this partition has been read
            LOG_INFO("sstable write task finished ", K(ret), K(thread_id), K(task_id));
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to read partition row with unexpected error", KR(ret));
          }
        } else if (OB_FAIL(sstable_writer_.append_row(thread_id, *datum_row))) {
          LOG_WARN("fail to append to the sstable", K(ret), K(thread_id), K(task_id));
        } else {
          datum_row->~ObLoadDatumRow();
        }
      }

      datum_row_allocator.reset();
      LOG_INFO("TASK step 3",K(thread_id), K(task_id) ,K(all_size), K(ret));
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sstable_writer_.close_macro_block_writer(thread_id))) {
          LOG_WARN("fail to close the macro_block.", K(ret), K(thread_id), K(task_id));
        }
      }
      LOG_INFO("TASK step 4",K(thread_id), K(task_id) ,K(all_size), K(ret));
    }
    if (OB_FAIL(ret)) {
      ret_.store(ret);
    }
  }
  datum_row_allocator.reset();
}

/**
 * ObLoadDataDirect
 */

int ObLoadDataDirect::execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_init(load_stmt))) {
    LOG_WARN("fail to inner init", KR(ret));
  } else if (OB_FAIL(do_load())) {
    LOG_WARN("fail to do load", KR(ret));
  }
  return ret;
}

int ObLoadDataDirect::inner_init(ObLoadDataStmt &load_stmt)
{
  LOG_INFO("start init");
  const ObLoadArgument &load_args = load_stmt.get_load_arguments();
  const auto &field_or_var_list = load_stmt.get_field_or_var_list();
  const uint64_t tenant_id = load_args.tenant_id_;
  const uint64_t table_id = load_args.table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  std::string partition_directory = PARTITION_DIR + std::to_string(table_id);

  int ret = OB_SUCCESS;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
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
  // init partition_splitter_
  else if (OB_FAIL(partition_splitter_.init(load_stmt, table_schema, partition_directory))) {
    LOG_WARN("fail to init partition splitter", KR(ret));
  }
  // split data
  else if (OB_FAIL(partition_splitter_.split())) {
    LOG_WARN("fail to split partition", KR(ret));
  }
  // init sstable_writer_
  else if (OB_FAIL(sstable_writer_thread_pool_.init(table_schema, partition_directory, 0, PARTITION_NUM - 1))) {
    LOG_WARN("fail to init sstable writer", KR(ret));
  }
  return ret;
}

int ObLoadDataDirect::do_load()
{
  LOG_INFO("start load");
  int ret = OB_SUCCESS;
  sstable_writer_thread_pool_.set_thread_count(N_CPU);
  sstable_writer_thread_pool_.set_run_wrapper(MTL_CTX());
  sstable_writer_thread_pool_.start();
  sstable_writer_thread_pool_.wait();
  if (OB_SUCC(sstable_writer_thread_pool_.get_res())) {
    if (OB_FAIL(sstable_writer_thread_pool_.close())) {
      LOG_WARN("fail to close sstable writer", KR(ret));
    }
  }

  LOG_INFO("load done", KR(ret));
  return ret;
}

} // namespace sql
} // namespace oceanbase
