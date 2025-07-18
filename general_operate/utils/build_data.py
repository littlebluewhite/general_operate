from typing import Any


def compare_related_items(existing_items: list[dict],
                         new_items: list[Any],
                         foreign_key_field: str,
                         foreign_key_value: Any,
                         handle_missing_update: callable = None,
                         handle_missing_delete: callable = None) -> tuple[list[dict], list[dict], list[int]]:
    """
    通用的關聯項目比較函數，基於 ID 規則確定 CRUD 操作
    
    ID 規則：
    - id == 0 or None: 創建新記錄
    - id > 0: 更新現有記錄（如果存在）
    - id < 0: 刪除現有記錄，使用 abs(id)（如果存在）

    Args:
        existing_items: 現有項目的字典列表
        new_items: 新項目的對象列表
        foreign_key_field: 外鍵字段名稱
        foreign_key_value: 外鍵值
        handle_missing_update: 處理不存在更新目標的回調函數
        handle_missing_delete: 處理不存在刪除目標的回調函數

    Returns:
        tuple: (to_create, to_update, to_delete_ids)
        - to_create: 要創建的項目數據字典列表
        - to_update: 要更新的項目字典列表（包含 'id' 和更新字段）
        - to_delete_ids: 要刪除的項目 ID 列表
    """
    # 創建查找字典以提高效率
    existing_by_id = {item['id']: item for item in existing_items}

    to_create = []
    to_update = []
    to_delete_ids = []

    # 根據 ID 規則處理每個項目
    for item in new_items:
        if item.id == 0 or item.id is None:
            # 創建：添加到創建列表
            create_data = build_create_data(item)
            if create_data:  # 只有在有數據時才添加
                # 確保外鍵字段被設置
                create_data[foreign_key_field] = foreign_key_value
                to_create.append(create_data)

        elif item.id > 0:
            # 更新：檢查是否存在並有變化
            if item.id in existing_by_id:
                update_data = build_update_data(item, existing_by_id[item.id])
                if update_data:  # 只有在有變化時才添加
                    to_update.append(update_data)
            else:
                # 處理不存在的 ID
                if handle_missing_update:
                    handle_missing_update(item.id)

        elif item.id < 0:
            # 刪除：檢查目標是否存在
            actual_id = abs(item.id)
            if actual_id in existing_by_id:
                to_delete_ids.append(actual_id)
            else:
                # 處理不存在的 ID
                if handle_missing_delete:
                    handle_missing_delete(actual_id)

    return to_create, to_update, to_delete_ids


def extract_object_fields(obj: Any,
                          include_fields: list[str] | None = None,
                          exclude_fields: list[str] | None = None,
                          exclude_none: bool = True,
                          field_mapping: dict[str, str] | None = None) -> dict:
    """
    通用字段提取方法，支持動態字段配置

    Args:
        obj: 要提取字段的對象
        include_fields: 要包含的字段列表（None表示包含所有）
        exclude_fields: 要排除的字段列表
        exclude_none: 是否排除None值
        field_mapping: 字段名映射 {source_field: target_field}

    Returns:
        dict: 提取的字段字典
    """
    result = {}
    exclude_fields = exclude_fields or []
    field_mapping = field_mapping or {}

    # 獲取對象的所有屬性
    if hasattr(obj, '__dict__'):
        # 普通對象
        obj_fields = obj.__dict__
    elif hasattr(obj, 'model_dump'):
        # Pydantic 模型
        obj_fields = obj.model_dump()
    elif hasattr(obj, '_asdict'):
        # namedtuple
        obj_fields = obj._asdict()
    else:
        # 使用 dir() 作為後備方案
        obj_fields = {attr: getattr(obj, attr) for attr in dir(obj)
                      if not attr.startswith('_') and not callable(getattr(obj, attr))}

    # 處理字段
    for field_name, value in obj_fields.items():
        # 檢查是否應該包含此字段
        if include_fields is not None and field_name not in include_fields:
            continue
        if field_name in exclude_fields:
            continue
        if exclude_none and value is None:
            continue

        # 應用字段映射
        target_field_name = field_mapping.get(field_name, field_name)
        result[target_field_name] = value

    return result

def build_create_data(sub: Any,
                       include_fields: list[str] | None = None,
                       exclude_fields: list[str] | None = None) -> dict:
    """
    通用的創建數據構建方法

    Args:
        sub: subtable 對象
        include_fields: 要包含的字段列表
        exclude_fields: 要排除的字段列表（默認排除 'id'）

    Returns:
        dict: 創建數據字典
    """
    # 默認排除 id 字段
    default_exclude = ['id']
    if exclude_fields:
        exclude_fields.extend(default_exclude)
    else:
        exclude_fields = default_exclude

    return extract_object_fields(
        obj=sub,
        include_fields=include_fields,
        exclude_fields=exclude_fields,
        exclude_none=True
    )

def build_update_data(sub: Any,
                       existing: dict,
                       include_fields: list[str] | None = None,
                       exclude_fields: list[str] | None = None,
                       force_include_id: bool = True) -> dict:
    """
    通用的更新數據構建方法，只包含有變化的字段

    Args:
        sub: subtable 對象
        existing: 現有數據字典
        include_fields: 要包含的字段列表
        exclude_fields: 要排除的字段列表
        force_include_id: 是否強制包含 id 字段

    Returns:
        dict: 更新數據字典（只包含有變化的字段）
    """
    # 獲取新數據的所有字段
    new_fields = extract_object_fields(
        obj=sub,
        include_fields=include_fields,
        exclude_fields=exclude_fields,
        exclude_none=True
    )

    # 比較變化
    update_data = {}
    has_changes = False

    # 強制包含 id（如果存在且要求包含）
    if force_include_id and hasattr(sub, 'id') and sub.id is not None:
        update_data['id'] = sub.id

    # 檢查每個字段的變化
    for field_name, new_value in new_fields.items():
        existing_value = existing.get(field_name)
        if new_value != existing_value:
            update_data[field_name] = new_value
            has_changes = True

    return update_data if has_changes else {}
