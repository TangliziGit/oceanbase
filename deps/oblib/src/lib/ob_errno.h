
// Copyright (c) 2018 Alibaba Inc. All Rights Reserved.
//
/// \file errno.h
/// \date 2018-04-28
///

// DO NOT EDIT. This file is automatically generated from ob_errno.def.
// DO NOT EDIT. This file is automatically generated from ob_errno.def.
// DO NOT EDIT. This file is automatically generated from ob_errno.def.
// To add errno in this header file, you should use DEFINE_***_DEP to define errno in ob_errno.def
// For any question, call fyy280124
#ifndef OB_ERRNO_H
#define OB_ERRNO_H

namespace oceanbase {
namespace common {

constexpr int OB_MAX_ERROR_CODE                      = 65535;

constexpr int OB_SUCCESS = 0;
constexpr int OB_ERROR = -4000;
constexpr int OB_OBJ_TYPE_ERROR = -4001;
constexpr int OB_INVALID_ARGUMENT = -4002;
constexpr int OB_ARRAY_OUT_OF_RANGE = -4003;
constexpr int OB_SERVER_LISTEN_ERROR = -4004;
constexpr int OB_INIT_TWICE = -4005;
constexpr int OB_NOT_INIT = -4006;
constexpr int OB_NOT_SUPPORTED = -4007;
constexpr int OB_ITER_END = -4008;
constexpr int OB_IO_ERROR = -4009;
constexpr int OB_ERROR_FUNC_VERSION = -4010;
constexpr int OB_TASK_FINISH = -4011;
constexpr int OB_TIMEOUT = -4012;
constexpr int OB_ALLOCATE_MEMORY_FAILED = -4013;
constexpr int OB_INNER_STAT_ERROR = -4014;
constexpr int OB_ERR_SYS = -4015;
constexpr int OB_ERR_UNEXPECTED = -4016;
constexpr int OB_ENTRY_EXIST = -4017;
constexpr int OB_ENTRY_NOT_EXIST = -4018;
constexpr int OB_SIZE_OVERFLOW = -4019;
constexpr int OB_REF_NUM_NOT_ZERO = -4020;
constexpr int OB_CONFLICT_VALUE = -4021;
constexpr int OB_ITEM_NOT_SETTED = -4022;
constexpr int OB_EAGAIN = -4023;
constexpr int OB_BUF_NOT_ENOUGH = -4024;
constexpr int OB_READ_NOTHING = -4026;
constexpr int OB_FILE_NOT_EXIST = -4027;
constexpr int OB_DISCONTINUOUS_LOG = -4028;
constexpr int OB_SERIALIZE_ERROR = -4033;
constexpr int OB_DESERIALIZE_ERROR = -4034;
constexpr int OB_AIO_TIMEOUT = -4035;
constexpr int OB_NEED_RETRY = -4036;
constexpr int OB_NOT_MASTER = -4038;
constexpr int OB_DECRYPT_FAILED = -4041;
constexpr int OB_NOT_THE_OBJECT = -4050;
constexpr int OB_LAST_LOG_RUINNED = -4052;
constexpr int OB_INVALID_ERROR = -4055;
constexpr int OB_DECIMAL_OVERFLOW_WARN = -4057;
constexpr int OB_EMPTY_RANGE = -4063;
constexpr int OB_DIR_NOT_EXIST = -4066;
constexpr int OB_INVALID_DATA = -4070;
constexpr int OB_CANCELED = -4072;
constexpr int OB_LOG_NOT_ALIGN = -4074;
constexpr int OB_NOT_IMPLEMENT = -4077;
constexpr int OB_DIVISION_BY_ZERO = -4078;
constexpr int OB_EXCEED_MEM_LIMIT = -4080;
constexpr int OB_QUEUE_OVERFLOW = -4085;
constexpr int OB_START_LOG_CURSOR_INVALID = -4099;
constexpr int OB_LOCK_NOT_MATCH = -4100;
constexpr int OB_DEAD_LOCK = -4101;
constexpr int OB_CHECKSUM_ERROR = -4103;
constexpr int OB_INIT_FAIL = -4104;
constexpr int OB_ROWKEY_ORDER_ERROR = -4105;
constexpr int OB_PHYSIC_CHECKSUM_ERROR = -4108;
constexpr int OB_STATE_NOT_MATCH = -4109;
constexpr int OB_IN_STOP_STATE = -4114;
constexpr int OB_LOG_NOT_CLEAR = -4116;
constexpr int OB_FILE_ALREADY_EXIST = -4117;
constexpr int OB_UNKNOWN_PACKET = -4118;
constexpr int OB_RPC_PACKET_TOO_LONG = -4119;
constexpr int OB_LOG_TOO_LARGE = -4120;
constexpr int OB_RPC_SEND_ERROR = -4121;
constexpr int OB_RPC_POST_ERROR = -4122;
constexpr int OB_LIBEASY_ERROR = -4123;
constexpr int OB_CONNECT_ERROR = -4124;
constexpr int OB_RPC_PACKET_INVALID = -4128;
constexpr int OB_BAD_ADDRESS = -4144;
constexpr int OB_ERR_MIN_VALUE = -4150;
constexpr int OB_ERR_MAX_VALUE = -4151;
constexpr int OB_ERR_NULL_VALUE = -4152;
constexpr int OB_RESOURCE_OUT = -4153;
constexpr int OB_ERR_SQL_CLIENT = -4154;
constexpr int OB_OPERATE_OVERFLOW = -4157;
constexpr int OB_INVALID_DATE_FORMAT = -4158;
constexpr int OB_INVALID_ARGUMENT_NUM = -4161;
constexpr int OB_EMPTY_RESULT = -4165;
constexpr int OB_LOG_INVALID_MOD_ID = -4168;
constexpr int OB_LOG_MODULE_UNKNOWN = -4169;
constexpr int OB_LOG_LEVEL_INVALID = -4170;
constexpr int OB_LOG_PARSER_SYNTAX_ERR = -4171;
constexpr int OB_UNKNOWN_CONNECTION = -4174;
constexpr int OB_ERROR_OUT_OF_RANGE = -4175;
constexpr int OB_OP_NOT_ALLOW = -4179;
constexpr int OB_ERR_ALREADY_EXISTS = -4181;
constexpr int OB_SEARCH_NOT_FOUND = -4182;
constexpr int OB_ITEM_NOT_MATCH = -4187;
constexpr int OB_INVALID_DATE_FORMAT_END = -4190;
constexpr int OB_HASH_EXIST = -4200;
constexpr int OB_HASH_NOT_EXIST = -4201;
constexpr int OB_HASH_GET_TIMEOUT = -4204;
constexpr int OB_HASH_PLACEMENT_RETRY = -4205;
constexpr int OB_HASH_FULL = -4206;
constexpr int OB_WAIT_NEXT_TIMEOUT = -4208;
constexpr int OB_MAJOR_FREEZE_NOT_FINISHED = -4213;
constexpr int OB_INVALID_DATE_VALUE = -4219;
constexpr int OB_INACTIVE_SQL_CLIENT = -4220;
constexpr int OB_INACTIVE_RPC_PROXY = -4221;
constexpr int OB_INTERVAL_WITH_MONTH = -4222;
constexpr int OB_TOO_MANY_DATETIME_PARTS = -4223;
constexpr int OB_DATA_OUT_OF_RANGE = -4224;
constexpr int OB_ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD = -4226;
constexpr int OB_ERR_OUT_OF_LOWER_BOUND = -4233;
constexpr int OB_ERR_OUT_OF_UPPER_BOUND = -4234;
constexpr int OB_BAD_NULL_ERROR = -4235;
constexpr int OB_FILE_NOT_OPENED = -4243;
constexpr int OB_ERR_DATA_TRUNCATED = -4249;
constexpr int OB_NOT_RUNNING = -4250;
constexpr int OB_ERR_COMPRESS_DECOMPRESS_DATA = -4257;
constexpr int OB_ERR_INCORRECT_STRING_VALUE = -4258;
constexpr int OB_DATETIME_FUNCTION_OVERFLOW = -4261;
constexpr int OB_ERR_DOUBLE_TRUNCATED = -4262;
constexpr int OB_CACHE_FREE_BLOCK_NOT_ENOUGH = -4273;
constexpr int OB_LAST_LOG_NOT_COMPLETE = -4278;
constexpr int OB_ERR_INTERVAL_PARTITION_EXIST = -4728;
constexpr int OB_ERR_INTERVAL_PARTITION_ERROR = -4729;
constexpr int OB_FROZEN_INFO_ALREADY_EXIST = -4744;
constexpr int OB_ERR_PARSER_SYNTAX = -5006;
constexpr int OB_ERR_COLUMN_NOT_FOUND = -5031;
constexpr int OB_ERR_SYS_VARIABLE_UNKNOWN = -5044;
constexpr int OB_ERR_READ_ONLY = -5081;
constexpr int OB_INTEGER_PRECISION_OVERFLOW = -5088;
constexpr int OB_DECIMAL_PRECISION_OVERFLOW = -5089;
constexpr int OB_NUMERIC_OVERFLOW = -5093;
constexpr int OB_ERR_SYS_CONFIG_UNKNOWN = -5099;
constexpr int OB_INVALID_ARGUMENT_FOR_EXTRACT = -5106;
constexpr int OB_INVALID_ARGUMENT_FOR_IS = -5107;
constexpr int OB_INVALID_ARGUMENT_FOR_LENGTH = -5108;
constexpr int OB_INVALID_ARGUMENT_FOR_SUBSTR = -5109;
constexpr int OB_INVALID_ARGUMENT_FOR_TIME_TO_USEC = -5110;
constexpr int OB_INVALID_ARGUMENT_FOR_USEC_TO_TIME = -5111;
constexpr int OB_INVALID_NUMERIC = -5114;
constexpr int OB_ERR_REGEXP_ERROR = -5115;
constexpr int OB_ERR_UNKNOWN_CHARSET = -5142;
constexpr int OB_ERR_UNKNOWN_COLLATION = -5143;
constexpr int OB_ERR_COLLATION_MISMATCH = -5144;
constexpr int OB_ERR_WRONG_VALUE_FOR_VAR = -5145;
constexpr int OB_TENANT_NOT_IN_SERVER = -5150;
constexpr int OB_TENANT_NOT_EXIST = -5157;
constexpr int OB_ERR_DATA_TOO_LONG = -5167;
constexpr int OB_ERR_WRONG_VALUE_COUNT_ON_ROW = -5168;
constexpr int OB_CANT_AGGREGATE_2COLLATIONS = -5177;
constexpr int OB_ERR_UNKNOWN_TIME_ZONE = -5192;
constexpr int OB_ERR_TOO_BIG_PRECISION = -5203;
constexpr int OB_ERR_M_BIGGER_THAN_D = -5204;
constexpr int OB_ERR_TRUNCATED_WRONG_VALUE = -5222;
constexpr int OB_ERR_WRONG_VALUE = -5241;
constexpr int OB_ERR_UNEXPECTED_TZ_TRANSITION = -5297;
constexpr int OB_ERR_INVALID_TIMEZONE_REGION_ID = -5341;
constexpr int OB_ERR_INVALID_HEX_NUMBER = -5342;
constexpr int OB_ERR_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL = -5352;
constexpr int OB_ERR_INVALID_JSON_TEXT = -5411;
constexpr int OB_ERR_INVALID_JSON_TEXT_IN_PARAM = -5412;
constexpr int OB_ERR_INVALID_JSON_BINARY_DATA = -5413;
constexpr int OB_ERR_INVALID_JSON_PATH = -5414;
constexpr int OB_ERR_INVALID_JSON_CHARSET = -5415;
constexpr int OB_ERR_INVALID_JSON_CHARSET_IN_FUNCTION = -5416;
constexpr int OB_ERR_INVALID_TYPE_FOR_JSON = -5417;
constexpr int OB_ERR_INVALID_CAST_TO_JSON = -5418;
constexpr int OB_ERR_INVALID_JSON_PATH_CHARSET = -5419;
constexpr int OB_ERR_INVALID_JSON_PATH_WILDCARD = -5420;
constexpr int OB_ERR_JSON_VALUE_TOO_BIG = -5421;
constexpr int OB_ERR_JSON_KEY_TOO_BIG = -5422;
constexpr int OB_ERR_JSON_USED_AS_KEY = -5423;
constexpr int OB_ERR_JSON_VACUOUS_PATH = -5424;
constexpr int OB_ERR_JSON_BAD_ONE_OR_ALL_ARG = -5425;
constexpr int OB_ERR_NUMERIC_JSON_VALUE_OUT_OF_RANGE = -5426;
constexpr int OB_ERR_INVALID_JSON_VALUE_FOR_CAST = -5427;
constexpr int OB_ERR_JSON_OUT_OF_DEPTH = -5428;
constexpr int OB_ERR_JSON_DOCUMENT_NULL_KEY = -5429;
constexpr int OB_ERR_BLOB_CANT_HAVE_DEFAULT = -5430;
constexpr int OB_ERR_INVALID_JSON_PATH_ARRAY_CELL = -5431;
constexpr int OB_ERR_MISSING_JSON_VALUE = -5432;
constexpr int OB_ERR_MULTIPLE_JSON_VALUES = -5433;
constexpr int OB_INVALID_ARGUMENT_FOR_TIMESTAMP_TO_SCN = -5436;
constexpr int OB_INVALID_ARGUMENT_FOR_SCN_TO_TIMESTAMP = -5437;
constexpr int OB_ERR_YEAR_CONFLICTS_WITH_JULIAN_DATE = -5629;
constexpr int OB_ERR_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE = -5630;
constexpr int OB_ERR_MONTH_CONFLICTS_WITH_JULIAN_DATE = -5631;
constexpr int OB_ERR_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE = -5632;
constexpr int OB_ERR_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE = -5633;
constexpr int OB_ERR_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY = -5634;
constexpr int OB_ERR_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY = -5635;
constexpr int OB_ERR_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY = -5636;
constexpr int OB_ERR_DATE_NOT_VALID_FOR_MONTH_SPECIFIED = -5637;
constexpr int OB_ERR_INPUT_VALUE_NOT_LONG_ENOUGH = -5638;
constexpr int OB_ERR_INVALID_YEAR_VALUE = -5639;
constexpr int OB_ERR_INVALID_QUARTER_VALUE = -5640;
constexpr int OB_ERR_INVALID_MONTH = -5641;
constexpr int OB_ERR_INVALID_DAY_OF_THE_WEEK = -5642;
constexpr int OB_ERR_INVALID_DAY_OF_YEAR_VALUE = -5643;
constexpr int OB_ERR_INVALID_HOUR12_VALUE = -5644;
constexpr int OB_ERR_INVALID_HOUR24_VALUE = -5645;
constexpr int OB_ERR_INVALID_MINUTES_VALUE = -5646;
constexpr int OB_ERR_INVALID_SECONDS_VALUE = -5647;
constexpr int OB_ERR_INVALID_SECONDS_IN_DAY_VALUE = -5648;
constexpr int OB_ERR_INVALID_JULIAN_DATE_VALUE = -5649;
constexpr int OB_ERR_AM_OR_PM_REQUIRED = -5650;
constexpr int OB_ERR_BC_OR_AD_REQUIRED = -5651;
constexpr int OB_ERR_FORMAT_CODE_APPEARS_TWICE = -5652;
constexpr int OB_ERR_DAY_OF_WEEK_SPECIFIED_MORE_THAN_ONCE = -5653;
constexpr int OB_ERR_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD = -5654;
constexpr int OB_ERR_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR = -5655;
constexpr int OB_ERR_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE = -5656;
constexpr int OB_ERR_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE = -5657;
constexpr int OB_ERR_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT = -5658;
constexpr int OB_ERR_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT = -5659;
constexpr int OB_ERR_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE = -5660;
constexpr int OB_ERR_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE = -5661;
constexpr int OB_ERR_FORMAT_CODE_CANNOT_APPEAR = -5662;
constexpr int OB_ERR_NON_NUMERIC_CHARACTER_VALUE = -5663;
constexpr int OB_INVALID_MERIDIAN_INDICATOR_USE = -5664;
constexpr int OB_ERR_DAY_OF_MONTH_RANGE = -5667;
constexpr int OB_ERR_ARGUMENT_OUT_OF_RANGE = -5674;
constexpr int OB_ERR_INTERVAL_INVALID = -5676;
constexpr int OB_ERR_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL = -5708;
constexpr int OB_ERR_INVALID_TIME_ZONE_HOUR = -5709;
constexpr int OB_ERR_INVALID_TIME_ZONE_MINUTE = -5710;
constexpr int OB_ERR_NOT_A_VALID_TIME_ZONE = -5711;
constexpr int OB_ERR_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER = -5712;
constexpr int OB_ERR_OPERATOR_CANNOT_BE_USED_WITH_LIST = -5729;
constexpr int OB_INVALID_ROWID = -5802;
constexpr int OB_ERR_NUMERIC_NOT_MATCH_FORMAT_LENGTH = -5873;
constexpr int OB_ERR_DATETIME_INTERVAL_INTERNAL_ERROR = -5898;
constexpr int OB_ERR_DBLINK_NO_LIB = -5976;
constexpr int OB_SWITCHING_TO_FOLLOWER_GRACEFULLY = -6202;
constexpr int OB_MASK_SET_NO_NODE = -6203;
constexpr int OB_TRANS_TIMEOUT = -6210;
constexpr int OB_TRANS_KILLED = -6211;
constexpr int OB_TRANS_STMT_TIMEOUT = -6212;
constexpr int OB_TRANS_CTX_NOT_EXIST = -6213;
constexpr int OB_TRANS_UNKNOWN = -6225;
constexpr int OB_ERR_READ_ONLY_TRANSACTION = -6226;
constexpr int OB_PACKET_CLUSTER_ID_NOT_MATCH = -8004;
constexpr int OB_URI_ERROR = -9001;
constexpr int OB_FINAL_MD5_ERROR = -9002;
constexpr int OB_OSS_ERROR = -9003;
constexpr int OB_INIT_MD5_ERROR = -9004;
constexpr int OB_OUT_OF_ELEMENT = -9005;
constexpr int OB_UPDATE_MD5_ERROR = -9006;
constexpr int OB_FILE_LENGTH_INVALID = -9007;
constexpr int OB_BACKUP_FILE_NOT_EXIST = -9011;
constexpr int OB_INVALID_BACKUP_DEST = -9026;
constexpr int OB_COS_ERROR = -9060;
constexpr int OB_IO_LIMIT = -9061;
constexpr int OB_BACKUP_BACKUP_REACH_COPY_LIMIT = -9062;
constexpr int OB_BACKUP_IO_PROHIBITED = -9063;
constexpr int OB_BACKUP_PERMISSION_DENIED = -9071;
constexpr int OB_ESI_OBS_ERROR = -9073;
constexpr int OB_BACKUP_META_INDEX_NOT_EXIST = -9076;
constexpr int OB_BACKUP_DEVICE_OUT_OF_SPACE = -9082;
constexpr int OB_BACKUP_PWRITE_OFFSET_NOT_MATCH = -9083;
constexpr int OB_BACKUP_PWRITE_CONTENT_NOT_MATCH = -9084;
constexpr int OB_ERR_XSLT_PARSE = -9574;
constexpr int OB_MAX_RAISE_APPLICATION_ERROR         = -20000;
constexpr int OB_MIN_RAISE_APPLICATION_ERROR         = -20999;

} // common
using namespace common; // maybe someone can fix
} // oceanbase

#endif /* OB_ERRNO_H */
