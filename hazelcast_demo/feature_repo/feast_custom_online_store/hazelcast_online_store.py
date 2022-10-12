import logging
from datetime import datetime
from typing import Sequence, List, Optional, Tuple, Dict, Callable, Any
import pytz

from feast import RepoConfig, FeatureView, Entity
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.online_stores.online_store import OnlineStore
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
import hazelcast

from feast.repo_config import FeastConfigBaseModel

from pydantic import StrictStr


class HazelcastOnlineStoreConfig(FeastConfigBaseModel):
    """Online store config for Hazelcast store"""

    type = "feast_custom_online_store.hazelcast_online_store.HazelcastOnlineStore"
    cluster_name: Optional[StrictStr] = "dev"
    cluster_members: Optional[StrictStr] = "localhost:5701"


class HazelcastOnlineStore(OnlineStore):
    _client: Optional[hazelcast.HazelcastClient] = None

    def _get_client(self, config: RepoConfig):
        if not self._client:
            self._client = hazelcast.HazelcastClient(
                cluster_members=[config.online_store.cluster_members],
                cluster_name=config.online_store.cluster_name,
            )
            logging.basicConfig(level=logging.ERROR)

        return self._client

    def online_write_batch(
            self,
            config: RepoConfig,
            table: FeatureView,
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            progress: Optional[Callable[[int], Any]]
    ) -> None:
        client = self._get_client(config)
        project = config.project

        for entity_key, values, timestamp, created_ts in data:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=2,
            ).hex()
            timestamp = _to_naive_utc(timestamp)
            if created_ts is not None:
                created_ts = _to_naive_utc(created_ts)

            for feature_name, val in values.items():
                client.sql.execute(f"""
                    SINK INTO {_table_id(project, table)}
                    (entity_key, feature_name, feature_value, event_ts, created_ts)
                    values (?, ?, ?, ?, ?);
                """, entity_key_bin, feature_name, val.SerializeToString().hex(), timestamp, created_ts).result()
            if progress:
                progress(1)

    def online_read(
            self,
            config: RepoConfig,
            table: FeatureView,
            entity_keys: List[EntityKeyProto],
            requested_features: Optional[List[str]] = None
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:

        client = self._get_client(config)
        project = config.project

        result: List[Tuple[Optional[datetime], Optional[Dict[str, Any]]]] = []

        for entity_key in entity_keys:
            entity_key_bin = serialize_entity_key(
                entity_key,
                entity_key_serialization_version=2,
            ).hex()

            records = client.sql.execute(f"""
                SELECT feature_name, feature_value, event_ts FROM {_table_id(project, table)} WHERE entity_key=?""",
                entity_key_bin
            ).result()
            res = {}
            res_ts: Optional[datetime] = None
            if records:
                for row in records:
                    val = ValueProto()
                    val.ParseFromString(bytes.fromhex(row["feature_value"]))
                    res[row["feature_name"]] = val
                    res_ts = row["event_ts"]
            if not res:
                result.append((None, None))
            else:
                result.append((res_ts, res))
        return result

    def update(
            self,
            config: RepoConfig,
            tables_to_delete: Sequence[FeatureView],
            tables_to_keep: Sequence[FeatureView],
            entities_to_delete: Sequence[Entity],
            entities_to_keep: Sequence[Entity],
            partial: bool
    ):
        client = self._get_client(config)
        project = config.project
        for table in tables_to_keep:
            client.sql.execute(
                f"""CREATE MAPPING IF NOT EXISTS {_table_id(project, table)} (
                        entity_key VARCHAR EXTERNAL NAME "__key.entity_key",
                        feature_name VARCHAR EXTERNAL NAME "__key.feature_name",
                        feature_value OBJECT,
                        event_ts TIMESTAMP WITH TIME ZONE,
                        created_ts TIMESTAMP WITH TIME ZONE
                    )
                    TYPE IMap
                    OPTIONS (
                        'keyFormat' = 'json-flat',
                        'valueFormat' = 'json-flat'
                    )
                """
            ).result()

        for table in tables_to_delete:
            client.sql.execute(f"DELETE FROM {_table_id(config.project, table)}").result()
            client.sql.execute(f"DROP MAPPING IF EXISTS {_table_id(config.project, table)}").result()

    def teardown(
            self,
            config: RepoConfig,
            tables: Sequence[FeatureView],
            entities: Sequence[Entity]
    ):
        client = self._get_client(config)
        project = config.project

        for table in tables:
            client.sql.execute(f"DELETE FROM {_table_id(config.project, table)}")
            client.sql.execute(f"DROP MAPPING IF EXISTS {_table_id(project, table)}")


def _table_id(project: str, table: FeatureView) -> str:
    return f"{project}_{table.name}"


def _to_naive_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts
    else:
        return ts.astimezone(pytz.utc).replace(tzinfo=None)
