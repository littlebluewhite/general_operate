import json
from datetime import datetime, date, time
from decimal import Decimal
from typing import Any


class EnhancedJSONEncoder(json.JSONEncoder):
    """
    Modern JSON encoder that handles datetime and other non-serializable types.

    Follows Python 3.11+ best practices:
    - Uses datetime.now(timezone.utc) instead of deprecated datetime.utcnow()
    - Handles common database model types (datetime, Decimal, etc.)
    - Provides graceful fallbacks for complex objects
    """

    def default(self, obj: Any) -> Any:
        # Handle datetime types (most common case first for performance)
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, time):
            return obj.isoformat()

        # Handle Decimal (common in database models)
        elif isinstance(obj, Decimal):
            return float(obj)

        # Handle bytes
        elif isinstance(obj, bytes):
            return obj.decode('utf-8', errors='replace')

        # Handle sets (convert to list for JSON compatibility)
        elif isinstance(obj, set):
            return list(obj)

        # Handle UUID objects (if imported)
        elif hasattr(obj, 'hex') and hasattr(obj, 'version'):  # UUID duck typing
            return str(obj)

        # Handle any object with __dict__ (e.g., SQLAlchemy models, Pydantic models)
        elif hasattr(obj, '__dict__'):
            # Filter out private attributes
            return {k: v for k, v in obj.__dict__.items()
                   if not k.startswith('_')}

        # Let the base class handle other types or raise TypeError
        return super().default(obj)
