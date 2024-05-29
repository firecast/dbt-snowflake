from dataclasses import dataclass
from typing import Optional, Dict, Any

import agate
from dbt.adapters.relation_configs import RelationConfigChange, RelationResults
from dbt.adapters.contracts.relation import RelationConfig
from dbt.adapters.contracts.relation import ComponentName

from dbt.adapters.snowflake.relation_configs.base import SnowflakeRelationConfigBase


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeIcebergTableConfig(SnowflakeRelationConfigBase):
    """
    This config follow the specs found here:
    https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table

    The following parameters are configurable by dbt:
    - name: name of the iceberg table
    - query: the query behind the table
    - external_volume: the external volume that provides the storage for the iceberg table
    - catalog: the name of the catalog that stores the iceberg table
    - base_location: the base location for the iceberg table in the external volume

    There are currently no non-configurable parameters.
    """

    name: str
    schema_name: str
    database_name: str
    query: str
    external_volume: str
    catalog: str
    base_location: str

    @classmethod
    def from_dict(cls, config_dict) -> "SnowflakeIcebergTableConfig":
        kwargs_dict = {
            "name": cls._render_part(ComponentName.Identifier, config_dict.get("name")),
            "schema_name": cls._render_part(ComponentName.Schema, config_dict.get("schema_name")),
            "database_name": cls._render_part(
                ComponentName.Database, config_dict.get("database_name")
            ),
            "query": config_dict.get("query"),
            "external_volume": config_dict.get("external_volume"),
            "catalog": config_dict.get("catalog"),
            "base_location": config_dict.get("base_location"),
        }

        iceberg_table: "SnowflakeIcebergTableConfig" = super().from_dict(kwargs_dict)
        return iceberg_table

    @classmethod
    def parse_relation_config(cls, relation_config: RelationConfig) -> Dict[str, Any]:
        config_dict = {
            "name": relation_config.identifier,
            "schema_name": relation_config.schema,
            "database_name": relation_config.database,
            "query": relation_config.compiled_code,
            "external_volume": relation_config.config.extra.get("external_volume"),
            "catalog": relation_config.config.extra.get("catalog"),
            "base_location": relation_config.config.extra.get("base_location"),
        }

        return config_dict

    @classmethod
    def parse_relation_results(cls, relation_results: RelationResults) -> Dict:
        iceberg_table: agate.Row = relation_results["iceberg_table"].rows[0]

        config_dict = {
            "name": iceberg_table.get("name"),
            "schema_name": iceberg_table.get("schema_name"),
            "database_name": iceberg_table.get("database_name"),
            "query": iceberg_table.get("text"),
            "external_volume": iceberg_table.get("external_volume"),
            "catalog": iceberg_table.get("catalog"),
            "base_location": iceberg_table.get("base_location"),
        }

        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeIcebergTableExternalVolumeConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeIcebergTableCatalogConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SnowflakeIcebergTableBaseLocationConfigChange(RelationConfigChange):
    context: Optional[str] = None

    @property
    def requires_full_refresh(self) -> bool:
        return True


@dataclass
class SnowflakeIcebergTableConfigChangeset:
    external_volume: Optional[SnowflakeIcebergTableExternalVolumeConfigChange] = None
    catalog: Optional[SnowflakeIcebergTableCatalogConfigChange] = None
    base_location: Optional[SnowflakeIcebergTableBaseLocationConfigChange] = None

    @property
    def requires_full_refresh(self) -> bool:
        return any(
            [
                self.external_volume.requires_full_refresh if self.external_volume else False,
                (self.catalog.requires_full_refresh if self.catalog else False),
                (self.base_location.requires_full_refresh if self.base_location else False),
            ]
        )

    @property
    def has_changes(self) -> bool:
        return any([self.external_volume, self.catalog, self.base_location])
