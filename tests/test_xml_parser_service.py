import tempfile
import unittest
from pathlib import Path

from server.services.sql.xml_parser_service import parse_single_mapper_xml


class XmlParserServiceTest(unittest.TestCase):
    def _parse_mapper_sql(self, mapper_body: str) -> str:
        mapper_xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<mapper namespace="sample.Mapper">
  {mapper_body}
</mapper>
"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            xml_path = Path(tmp_dir) / "sample.xml"
            xml_path.write_text(mapper_xml, encoding="utf-8")
            items = parse_single_mapper_xml(xml_path)

        self.assertEqual(len(items), 1)
        return items[0].fr_sql_text

    def test_if_tail_text_is_not_duplicated(self):
        sql_text = self._parse_mapper_sql(
            """
<select id="selectWithIf">
  SELECT A
  FROM B
  WHERE 1=1
  <if test="cond != null">
    AND C = #{cond}
  </if>
  ORDER BY D
</select>
"""
        )

        self.assertEqual(sql_text.count("ORDER BY D"), 1)

    def test_foreach_tail_closing_parenthesis_is_not_duplicated(self):
        sql_text = self._parse_mapper_sql(
            """
<select id="selectWithForeach">
  SELECT A
  FROM B
  WHERE ID IN (
  <foreach collection="ids" item="id" separator=",">
    #{id}
  </foreach>
  )
</select>
"""
        )

        self.assertEqual(sql_text.rstrip().count(")"), 1)

    def test_choose_tail_order_by_is_not_duplicated(self):
        sql_text = self._parse_mapper_sql(
            """
<select id="selectWithChoose">
  SELECT A
  FROM B
  <choose>
    <when test="name != null">
      WHERE NAME = #{name}
    </when>
    <otherwise>
      WHERE 1=1
    </otherwise>
  </choose>
  ORDER BY A
</select>
"""
        )

        self.assertEqual(sql_text.count("ORDER BY A"), 1)


if __name__ == "__main__":
    unittest.main()
