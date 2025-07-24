"""
工具模組測試
測試 build_data.py 和 exception.py 的功能
"""

import pytest
from unittest.mock import Mock
from typing import Any
from dataclasses import dataclass
from collections import namedtuple

from general_operate.utils.build_data import (
    compare_related_items,
    extract_object_fields, 
    build_create_data,
    build_update_data
)
from general_operate.utils.exception import GeneralOperateException


# 測試用的數據結構
@dataclass
class TestItem:
    id: int
    name: str
    value: int
    description: str = None


class TestPydanticLikeModel:
    """模擬 Pydantic 模型"""
    def __init__(self, id: int, name: str, value: int):
        self.id = id
        self.name = name
        self.value = value
    
    def model_dump(self):
        return {"id": self.id, "name": self.name, "value": self.value}


TestNamedTuple = namedtuple('TestNamedTuple', ['id', 'name', 'value'])


class TestPlainObject:
    """普通對象"""
    def __init__(self, id: int, name: str, value: int):
        self.id = id
        self.name = name
        self.value = value


class TestCompareRelatedItems:
    """測試 compare_related_items 函數"""
    
    def test_create_items_with_zero_id(self):
        """測試創建項目（ID為0）"""
        existing_items = []
        new_items = [
            TestItem(id=0, name="item1", value=100),
            TestItem(id=0, name="item2", value=200)
        ]
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1
        )
        
        assert len(to_create) == 2
        assert len(to_update) == 0
        assert len(to_delete) == 0
        
        # 檢查創建數據
        assert to_create[0]["name"] == "item1"
        assert to_create[0]["value"] == 100
        assert to_create[0]["parent_id"] == 1
        assert "id" not in to_create[0]  # ID 應該被排除
    
    def test_create_items_with_none_id(self):
        """測試創建項目（ID為None）"""
        existing_items = []
        new_items = [TestItem(id=None, name="item1", value=100)]
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1
        )
        
        assert len(to_create) == 1
        assert to_create[0]["name"] == "item1"
        assert to_create[0]["parent_id"] == 1
    
    def test_update_existing_items(self):
        """測試更新現有項目"""
        existing_items = [
            {"id": 1, "name": "item1", "value": 100},
            {"id": 2, "name": "item2", "value": 200}
        ]
        new_items = [
            TestItem(id=1, name="updated_item1", value=150),
            TestItem(id=2, name="item2", value=200)  # 無變化
        ]
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1
        )
        
        assert len(to_create) == 0
        assert len(to_update) == 1  # 只有一個有變化
        assert len(to_delete) == 0
        
        # 檢查更新數據
        assert to_update[0]["id"] == 1
        assert to_update[0]["name"] == "updated_item1"
        assert to_update[0]["value"] == 150
    
    def test_delete_items_with_negative_id(self):
        """測試刪除項目（負數ID）"""
        existing_items = [
            {"id": 1, "name": "item1", "value": 100},
            {"id": 2, "name": "item2", "value": 200}
        ]
        new_items = [
            TestItem(id=-1, name="", value=0),  # 刪除 ID 1
            TestItem(id=-2, name="", value=0)   # 刪除 ID 2
        ]
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1
        )
        
        assert len(to_create) == 0
        assert len(to_update) == 0
        assert len(to_delete) == 2
        assert 1 in to_delete
        assert 2 in to_delete
    
    def test_mixed_operations(self):
        """測試混合操作（創建、更新、刪除）"""
        existing_items = [
            {"id": 1, "name": "item1", "value": 100},
            {"id": 2, "name": "item2", "value": 200},
            {"id": 3, "name": "item3", "value": 300}
        ]
        new_items = [
            TestItem(id=0, name="new_item", value=400),      # 創建
            TestItem(id=1, name="updated_item1", value=150), # 更新
            TestItem(id=2, name="item2", value=200),         # 無變化
            TestItem(id=-3, name="", value=0)                # 刪除
        ]
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1
        )
        
        assert len(to_create) == 1
        assert len(to_update) == 1
        assert len(to_delete) == 1
        
        assert to_create[0]["name"] == "new_item"
        assert to_update[0]["id"] == 1
        assert 3 in to_delete
    
    def test_handle_missing_update_callback(self):
        """測試處理不存在的更新目標回調"""
        existing_items = [{"id": 1, "name": "item1", "value": 100}]
        new_items = [TestItem(id=999, name="nonexistent", value=0)]  # 不存在的ID
        
        missing_update_ids = []
        def handle_missing_update(missing_id):
            missing_update_ids.append(missing_id)
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1,
            handle_missing_update=handle_missing_update
        )
        
        assert len(to_create) == 0
        assert len(to_update) == 0
        assert len(to_delete) == 0
        assert 999 in missing_update_ids
    
    def test_handle_missing_delete_callback(self):
        """測試處理不存在的刪除目標回調"""
        existing_items = [{"id": 1, "name": "item1", "value": 100}]
        new_items = [TestItem(id=-999, name="", value=0)]  # 不存在的ID
        
        missing_delete_ids = []
        def handle_missing_delete(missing_id):
            missing_delete_ids.append(missing_id)
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1,
            handle_missing_delete=handle_missing_delete
        )
        
        assert len(to_create) == 0
        assert len(to_update) == 0
        assert len(to_delete) == 0
        assert 999 in missing_delete_ids
    
    def test_empty_create_data_filtered_out(self):
        """測試空的創建數據被過濾掉"""
        existing_items = []
        # 創建一個只有None值的項目
        item = TestItem(id=0, name=None, value=None, description=None)
        new_items = [item]
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1
        )
        
        # 由於所有字段都是None且exclude_none=True，創建數據應該為空
        assert len(to_create) == 0


class TestExtractObjectFields:
    """測試 extract_object_fields 函數"""
    
    def test_extract_from_dataclass(self):
        """測試從 dataclass 提取字段"""
        item = TestItem(id=1, name="test", value=100, description="desc")
        
        result = extract_object_fields(item)
        
        assert result["id"] == 1
        assert result["name"] == "test"
        assert result["value"] == 100
        assert result["description"] == "desc"
    
    def test_extract_from_pydantic_like_model(self):
        """測試從 Pydantic 式模型提取字段"""
        item = TestPydanticLikeModel(id=1, name="test", value=100)
        
        result = extract_object_fields(item)
        
        assert result["id"] == 1
        assert result["name"] == "test"
        assert result["value"] == 100
    
    def test_extract_from_namedtuple(self):
        """測試從 namedtuple 提取字段"""
        item = TestNamedTuple(id=1, name="test", value=100)
        
        result = extract_object_fields(item)
        
        assert result["id"] == 1
        assert result["name"] == "test"
        assert result["value"] == 100
    
    def test_extract_from_plain_object(self):
        """測試從普通對象提取字段"""
        item = TestPlainObject(id=1, name="test", value=100)
        
        result = extract_object_fields(item)
        
        assert result["id"] == 1
        assert result["name"] == "test"
        assert result["value"] == 100
    
    def test_include_fields_filter(self):
        """測試包含字段過濾"""
        item = TestItem(id=1, name="test", value=100, description="desc")
        
        result = extract_object_fields(item, include_fields=["name", "value"])
        
        assert "name" in result
        assert "value" in result
        assert "id" not in result
        assert "description" not in result
    
    def test_exclude_fields_filter(self):
        """測試排除字段過濾"""
        item = TestItem(id=1, name="test", value=100, description="desc")
        
        result = extract_object_fields(item, exclude_fields=["id", "description"])
        
        assert "name" in result
        assert "value" in result
        assert "id" not in result
        assert "description" not in result
    
    def test_exclude_none_values(self):
        """測試排除None值"""
        item = TestItem(id=1, name="test", value=100, description=None)
        
        result = extract_object_fields(item, exclude_none=True)
        
        assert "id" in result
        assert "name" in result
        assert "value" in result
        assert "description" not in result
    
    def test_include_none_values(self):
        """測試包含None值"""
        item = TestItem(id=1, name="test", value=100, description=None)
        
        result = extract_object_fields(item, exclude_none=False)
        
        assert "description" in result
        assert result["description"] is None
    
    def test_field_mapping(self):
        """測試字段映射"""
        item = TestItem(id=1, name="test", value=100, description="desc")
        
        mapping = {"name": "item_name", "value": "item_value"}
        result = extract_object_fields(item, field_mapping=mapping)
        
        assert "item_name" in result
        assert "item_value" in result
        assert result["item_name"] == "test"
        assert result["item_value"] == 100
        assert "name" not in result
        assert "value" not in result
    
    def test_combined_filters(self):
        """測試組合過濾條件"""
        item = TestItem(id=1, name="test", value=100, description=None)
        
        result = extract_object_fields(
            item,
            include_fields=["id", "name", "value", "description"],
            exclude_fields=["id"],
            exclude_none=True,
            field_mapping={"name": "item_name"}
        )
        
        assert "item_name" in result
        assert "value" in result
        assert "id" not in result
        assert "description" not in result
        assert result["item_name"] == "test"


class TestBuildCreateData:
    """測試 build_create_data 函數"""
    
    def test_build_create_data_excludes_id(self):
        """測試構建創建數據時排除ID"""
        item = TestItem(id=1, name="test", value=100, description="desc")
        
        result = build_create_data(item)
        
        assert "id" not in result
        assert result["name"] == "test"
        assert result["value"] == 100
        assert result["description"] == "desc"
    
    def test_build_create_data_with_include_fields(self):
        """測試指定包含字段的創建數據構建"""
        item = TestItem(id=1, name="test", value=100, description="desc")
        
        result = build_create_data(item, include_fields=["name", "value"])
        
        assert "name" in result
        assert "value" in result
        assert "id" not in result
        assert "description" not in result
    
    def test_build_create_data_with_additional_exclude_fields(self):
        """測試額外排除字段的創建數據構建"""
        item = TestItem(id=1, name="test", value=100, description="desc")
        
        result = build_create_data(item, exclude_fields=["description"])
        
        assert "name" in result
        assert "value" in result
        assert "id" not in result
        assert "description" not in result
    
    def test_build_create_data_excludes_none(self):
        """測試創建數據構建時排除None值"""
        item = TestItem(id=1, name="test", value=100, description=None)
        
        result = build_create_data(item)
        
        assert "name" in result
        assert "value" in result
        assert "description" not in result


class TestBuildUpdateData:
    """測試 build_update_data 函數"""
    
    def test_build_update_data_with_changes(self):
        """測試構建有變化的更新數據"""
        item = TestItem(id=1, name="updated_test", value=150, description="new_desc")
        existing = {"id": 1, "name": "test", "value": 100, "description": "desc"}
        
        result = build_update_data(item, existing)
        
        assert result["id"] == 1
        assert result["name"] == "updated_test"
        assert result["value"] == 150
        assert result["description"] == "new_desc"
    
    def test_build_update_data_no_changes(self):
        """測試構建無變化的更新數據"""
        item = TestItem(id=1, name="test", value=100, description="desc")
        existing = {"id": 1, "name": "test", "value": 100, "description": "desc"}
        
        result = build_update_data(item, existing)
        
        assert result == {}  # 無變化應返回空字典
    
    def test_build_update_data_partial_changes(self):
        """測試構建部分變化的更新數據"""
        item = TestItem(id=1, name="updated_test", value=100, description="desc")
        existing = {"id": 1, "name": "test", "value": 100, "description": "desc"}
        
        result = build_update_data(item, existing)
        
        assert result["id"] == 1
        assert result["name"] == "updated_test"
        assert "value" not in result or result["value"] == 100  # 無變化的字段可能被包含或排除
        assert "description" not in result or result["description"] == "desc"
    
    def test_build_update_data_without_force_include_id(self):
        """測試不強制包含ID的更新數據構建"""
        item = TestItem(id=1, name="updated_test", value=100, description="desc")
        existing = {"id": 1, "name": "test", "value": 100, "description": "desc"}
        
        result = build_update_data(item, existing, force_include_id=False)
        
        assert "name" in result
        assert result["name"] == "updated_test"
        # ID可能不在結果中，取決於是否有變化
    
    def test_build_update_data_with_include_fields(self):
        """測試指定包含字段的更新數據構建"""
        item = TestItem(id=1, name="updated_test", value=150, description="new_desc")
        existing = {"id": 1, "name": "test", "value": 100, "description": "desc"}
        
        result = build_update_data(item, existing, include_fields=["name", "value"])
        
        assert result["id"] == 1
        assert result["name"] == "updated_test"
        assert result["value"] == 150
        assert "description" not in result
    
    def test_build_update_data_with_exclude_fields(self):
        """測試排除字段的更新數據構建"""
        item = TestItem(id=1, name="updated_test", value=150, description="new_desc")
        existing = {"id": 1, "name": "test", "value": 100, "description": "desc"}
        
        result = build_update_data(item, existing, exclude_fields=["description"])
        
        assert result["id"] == 1
        assert result["name"] == "updated_test"
        assert result["value"] == 150
        assert "description" not in result


class TestGeneralOperateException:
    """測試 GeneralOperateException 異常類"""
    
    def test_exception_initialization(self):
        """測試異常初始化"""
        exc = GeneralOperateException(
            status_code=400,
            message_code=1001,
            message="Test error message"
        )
        
        assert exc.status_code == 400
        assert exc.message_code == 1001
        assert exc.message == "Test error message"
    
    def test_exception_with_empty_message(self):
        """測試空消息的異常"""
        exc = GeneralOperateException(
            status_code=500,
            message_code=2001
        )
        
        assert exc.status_code == 500
        assert exc.message_code == 2001
        assert exc.message == ""
    
    def test_exception_inheritance(self):
        """測試異常繼承"""
        exc = GeneralOperateException(400, 1001, "Test error")
        
        assert isinstance(exc, Exception)
        assert isinstance(exc, GeneralOperateException)
    
    def test_exception_raising(self):
        """測試異常拋出"""
        with pytest.raises(GeneralOperateException) as exc_info:
            raise GeneralOperateException(404, 3001, "Not found")
        
        assert exc_info.value.status_code == 404
        assert exc_info.value.message_code == 3001
        assert exc_info.value.message == "Not found"
    
    def test_exception_string_representation(self):
        """測試異常字符串表示"""
        exc = GeneralOperateException(400, 1001, "Bad request")
        
        # 異常的字符串表示應該包含消息
        exc_str = str(exc)
        assert "Bad request" in exc_str


class TestUtilsIntegration:
    """測試工具模組集成場景"""
    
    def test_full_crud_workflow(self):
        """測試完整的 CRUD 工作流程"""
        # 現有數據
        existing_items = [
            {"id": 1, "name": "item1", "value": 100},
            {"id": 2, "name": "item2", "value": 200},
            {"id": 3, "name": "item3", "value": 300}
        ]
        
        # 新的操作請求
        new_items = [
            TestItem(id=0, name="new_item", value=400),        # 創建
            TestItem(id=1, name="updated_item1", value=150),   # 更新
            TestItem(id=2, name="item2", value=200),           # 無變化
            TestItem(id=-3, name="", value=0)                  # 刪除
        ]
        
        # 執行比較
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1
        )
        
        # 驗證結果
        assert len(to_create) == 1
        assert len(to_update) == 1
        assert len(to_delete) == 1
        
        # 驗證創建數據
        create_item = to_create[0]
        assert create_item["name"] == "new_item"
        assert create_item["value"] == 400
        assert create_item["parent_id"] == 1
        assert "id" not in create_item
        
        # 驗證更新數據
        update_item = to_update[0]
        assert update_item["id"] == 1
        assert update_item["name"] == "updated_item1"
        assert update_item["value"] == 150
        
        # 驗證刪除數據
        assert 3 in to_delete
    
    def test_error_handling_integration(self):
        """測試錯誤處理集成"""
        existing_items = [{"id": 1, "name": "item1", "value": 100}]
        new_items = [
            TestItem(id=999, name="nonexistent_update", value=0),  # 不存在的更新
            TestItem(id=-888, name="", value=0)                    # 不存在的刪除
        ]
        
        errors = []
        
        def handle_missing_update(missing_id):
            errors.append(f"Update target not found: {missing_id}")
        
        def handle_missing_delete(missing_id):
            errors.append(f"Delete target not found: {missing_id}")
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1,
            handle_missing_update=handle_missing_update,
            handle_missing_delete=handle_missing_delete
        )
        
        # 應該沒有實際操作
        assert len(to_create) == 0
        assert len(to_update) == 0
        assert len(to_delete) == 0
        
        # 但應該記錄錯誤
        assert len(errors) == 2
        assert "Update target not found: 999" in errors
        assert "Delete target not found: 888" in errors
    
    def test_different_object_types_compatibility(self):
        """測試不同對象類型的兼容性"""
        existing_items = []
        
        # 混合不同對象類型
        new_items = [
            TestItem(id=0, name="dataclass_item", value=100),
            TestPydanticLikeModel(id=0, name="pydantic_item", value=200),
            TestPlainObject(id=0, name="plain_item", value=300)
        ]
        
        to_create, to_update, to_delete = compare_related_items(
            existing_items, new_items, "parent_id", 1
        )
        
        assert len(to_create) == 3
        
        # 驗證所有類型的對象都能正確處理
        names = [item["name"] for item in to_create]
        assert "dataclass_item" in names
        assert "pydantic_item" in names
        assert "plain_item" in names




if __name__ == "__main__":
    # 運行測試
    pytest.main([__file__, "-v"])