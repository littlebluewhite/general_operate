# Import original models for backward compatibility
from .tutorial import Tutorial, TutorialCreate, TutorialUpdate, TutorialBasic
from .subtable import Subtable, SubtableCreate, SubtableUpdate, SubtableBasic

# Import modules for GeneralOperate usage
from . import tutorial
from . import subtable

__all__ = [
    # Original models
    "Tutorial", "TutorialCreate", "TutorialUpdate", "TutorialBasic",
    "Subtable", "SubtableCreate", "SubtableUpdate", "SubtableBasic",

    
    # Modules
    "tutorial", "subtable"
]