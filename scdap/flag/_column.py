"""

@create on 2020.03.02

"""
from typing import List, Iterable
from scdap.data.feature_item.item_coder import FeatureItemKV
from scdap.data.feature_item.item import __feature_default__, __feature_key__

# 无用，向前兼容
ColumnItem = str


class Column(FeatureItemKV):

    def has_column(self, c: str):
        return c in __feature_default__

    def has_lrdata(self, select_column: Iterable[str]) -> bool:
        return len(set(select_column).intersection(self.lr_column)) > 0

    def has_all_lrdata(self, select_column: Iterable[str]) -> bool:
        return len(set(select_column).intersection(self.lr_column)) == 4

    def has_hrdata(self, select_column: Iterable[str]) -> bool:
        return len(set(select_column).intersection(self.hr_column)) > 0

    def has_all_hrdata(self, select_column: Iterable[str]) -> bool:
        return len(set(select_column).intersection(self.hr_column)) == 4

    def has_hrtime(self, select_column: Iterable[str]) -> bool:
        """
        确认是否拥有hrtime即高分特征时间, 只有在拥有高分特征时间的时候才需要

        :param select_column:
        :return:
        """
        return self.has_hrdata(select_column)

    # 所有特征数据
    @property
    def total_column(self):
        return __feature_key__

    # 四维低分数据
    @property
    def normal_column(self):
        return [self.meanhf, self.meanlf, self.mean, self.std]

    # 四维低分数据
    @property
    def lr_column(self):
        return self.normal_column

    # 四维高分数据
    @property
    def high_resolution_column(self):
        return [self.feature1, self.feature2, self.feature3, self.feature4]

    # 四维高分数据
    @property
    def hr_column(self):
        return self.high_resolution_column

    @staticmethod
    def format_column(select_column: Iterable[str]) -> List[str]:
        return format_column(select_column)


def convert_column(dist_column: FeatureItemKV, select_column: Iterable[str]) -> List[str]:
    """
    转换特征为dist_column中配置的字段名称

    :param dist_column:
    :param select_column:
    :return:
    """
    select_column = format_column(select_column)
    return [getattr(dist_column, k) for k in select_column]


def format_column(select_column: Iterable[str]) -> List[str]:
    """
    将所有字段都转正小写, 并且默认配置time和status字段, 另外也会更新是否存在高分特征来配置hrtime

    :param select_column:
    :return:
    """
    select_column = set(map(str.lower, select_column))

    if column.has_hrtime(select_column):
        select_column.add(column.hrtime)

    if column.time not in select_column:
        select_column.add(column.time)
    if column.status not in select_column:
        select_column.add(column.status)

    return list(select_column)


column = Column()
