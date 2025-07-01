from sqlalchemy import JSON, Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

VarcharString = String(512)


class Tutorial(Base):
    __tablename__ = "tutorial"

    id = Column(Integer, primary_key=True, index=True, autoincrement="auto")
    name = Column(VarcharString, nullable=False, unique=True)
    title = Column(VarcharString, default="")
    tags = Column(JSON, default=[], nullable=False)
    enable = Column(Boolean, default=True)

    subtable = relationship("Subtable", lazy="joined")


class Subtable(Base):
    __tablename__ = "subtable"

    id = Column(Integer, primary_key=True, index=True, autoincrement="auto")
    description = Column(VarcharString, default="")
    tutorial_id = Column(Integer, ForeignKey("tutorial.id"))
    author = Column(VarcharString, default="")
