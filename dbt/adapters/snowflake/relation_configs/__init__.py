from dbt.adapters.snowflake.relation_configs.dynamic_table import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableWarehouseConfigChange,
    SnowflakeDynamicTableTargetLagConfigChange,
)

from dbt.adapters.snowflake.relation_configs.iceberg_table import (
    SnowflakeIcebergTableConfig,
    SnowflakeIcebergTableConfigChangeset,
    SnowflakeIcebergTableExternalVolumeConfigChange,
    SnowflakeIcebergTableCatalogConfigChange,
    SnowflakeIcebergTableBaseLocationConfigChange,
)

from dbt.adapters.snowflake.relation_configs.policies import (
    SnowflakeIncludePolicy,
    SnowflakeQuotePolicy,
    SnowflakeRelationType,
)
