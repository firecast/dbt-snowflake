from dataclasses import dataclass, field
from typing import FrozenSet, Optional, Type

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import RelationConfigChangeAction, RelationResults
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.utils import classproperty

from dbt.adapters.snowflake.relation_configs import (
    SnowflakeDynamicTableConfig,
    SnowflakeDynamicTableConfigChangeset,
    SnowflakeDynamicTableTargetLagConfigChange,
    SnowflakeDynamicTableWarehouseConfigChange,
    SnowflakeIcebergTableConfig,
    SnowflakeIcebergTableConfigChangeset,
    SnowflakeIcebergTableBaseLocationConfigChange,
    SnowflakeIcebergTableCatalogConfigChange,
    SnowflakeIcebergTableExternalVolumeConfigChange,
    SnowflakeQuotePolicy,
    SnowflakeRelationType,
)


@dataclass(frozen=True, eq=False, repr=False)
class SnowflakeRelation(BaseRelation):
    type: Optional[SnowflakeRelationType] = None
    quote_policy: SnowflakeQuotePolicy = field(default_factory=lambda: SnowflakeQuotePolicy())
    require_alias: bool = False
    renameable_relations: FrozenSet[SnowflakeRelationType] = field(
        default_factory=lambda: frozenset(
            {
                SnowflakeRelationType.Table,  # type: ignore
                SnowflakeRelationType.View,  # type: ignore
            }
        )
    )

    replaceable_relations: FrozenSet[SnowflakeRelationType] = field(
        default_factory=lambda: frozenset(
            {
                SnowflakeRelationType.DynamicTable,  # type: ignore
                SnowflakeRelationType.IcebergTable,  # type: ignore
                SnowflakeRelationType.Table,  # type: ignore
                SnowflakeRelationType.View,  # type: ignore
            }
        )
    )

    @property
    def is_dynamic_table(self) -> bool:
        return self.type == SnowflakeRelationType.DynamicTable

    @property
    def is_iceberg_table(self) -> bool:
        return self.type == SnowflakeRelationType.IcebergTable

    @classproperty
    def DynamicTable(cls) -> str:
        return str(SnowflakeRelationType.DynamicTable)

    @classproperty
    def IcebergTable(cls) -> str:
        return str(SnowflakeRelationType.IcebergTable)

    @classproperty
    def get_relation_type(cls) -> Type[SnowflakeRelationType]:
        return SnowflakeRelationType

    @classmethod
    def dynamic_table_config_changeset(
        cls, relation_results: RelationResults, relation_config: RelationConfig
    ) -> Optional[SnowflakeDynamicTableConfigChangeset]:
        existing_dynamic_table = SnowflakeDynamicTableConfig.from_relation_results(
            relation_results
        )
        new_dynamic_table = SnowflakeDynamicTableConfig.from_relation_config(relation_config)

        config_change_collection = SnowflakeDynamicTableConfigChangeset()

        if new_dynamic_table.target_lag != existing_dynamic_table.target_lag:
            config_change_collection.target_lag = SnowflakeDynamicTableTargetLagConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_dynamic_table.target_lag,
            )

        if new_dynamic_table.snowflake_warehouse != existing_dynamic_table.snowflake_warehouse:
            config_change_collection.snowflake_warehouse = (
                SnowflakeDynamicTableWarehouseConfigChange(
                    action=RelationConfigChangeAction.alter,
                    context=new_dynamic_table.snowflake_warehouse,
                )
            )

        if config_change_collection.has_changes:
            return config_change_collection
        return None

    @classmethod
    def iceberg_table_config_changeset(
        cls, relation_results: RelationResults, relation_config: RelationConfig
    ) -> Optional[SnowflakeIcebergTableConfigChangeset]:
        existing_iceberg_table = SnowflakeIcebergTableConfig.from_relation_results(
            relation_results
        )
        new_iceberg_table = SnowflakeIcebergTableConfig.from_relation_config(relation_config)

        config_change_collection = SnowflakeIcebergTableConfigChangeset()

        if new_iceberg_table.external_volume != existing_iceberg_table.external_volume:
            config_change_collection.external_volume = (
                SnowflakeIcebergTableExternalVolumeConfigChange(
                    action=RelationConfigChangeAction.alter,
                    context=new_iceberg_table.external_volume,
                )
            )

        if new_iceberg_table.catalog != existing_iceberg_table.catalog:
            config_change_collection.catalog = SnowflakeIcebergTableCatalogConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_iceberg_table.catalog,
            )

        if new_iceberg_table.base_location != existing_iceberg_table.base_location:
            config_change_collection.base_location = SnowflakeIcebergTableBaseLocationConfigChange(
                action=RelationConfigChangeAction.alter,
                context=new_iceberg_table.base_location,
            )

        if config_change_collection.has_changes:
            return config_change_collection

        return None
