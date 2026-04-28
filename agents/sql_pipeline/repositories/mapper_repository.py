"""매핑 룰 조회 리포지토리."""

from agents.sql_pipeline.services.db_runtime import (
    get_connection,
    get_mapping_rule_detail_table,
    get_mapping_rule_table,
)
from agents.sql_pipeline.domain.models import MappingRuleItem


def _to_text(value, default: str = "") -> str:
    if value is None:
        return default
    if hasattr(value, "read"):
        value = value.read()
    if value is None:
        return default
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def get_all_mapping_rules() -> list[MappingRuleItem]:
    """NEXT_MIG_INFO + DTL 조인으로 전체 매핑 룰을 읽어온다."""
    map_table = get_mapping_rule_table()
    detail_table = get_mapping_rule_detail_table()
    query = f"""
        SELECT M.FR_TABLE, D.FR_COL, M.TO_TABLE, D.TO_COL
        FROM {map_table} M
        JOIN {detail_table} D
          ON M.MAP_ID = D.MAP_ID
        ORDER BY M.MAP_ID, D.MAP_DTL
    """

    rules: list[MappingRuleItem] = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        for row in cursor.fetchall():
            rules.append(
                MappingRuleItem(
                    map_type="",
                    fr_table=_to_text(row[0]),
                    fr_col=_to_text(row[1]),
                    to_table=_to_text(row[2]),
                    to_col=_to_text(row[3]),
                )
            )
    return rules
