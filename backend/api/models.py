"""Strict request shape; semantics and source mappings are validated separately."""
from typing import Any
from pydantic import BaseModel, ConfigDict, Field, model_validator

class QueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_id: str = Field(default="commerce", min_length=1, max_length=80, pattern=r"^[a-zA-Z0-9_-]+$")
    question: str | None = Field(default=None, min_length=1, max_length=1500)
    plan: dict[str, Any] | None = None
    catalog_version: str | None = Field(default=None, pattern=r"^[a-f0-9]{16}$")

    @model_validator(mode="after")
    def exactly_one_input(self):
        if (self.question is None) == (self.plan is None):
            raise ValueError("Provide either a question or a query plan.")
        if self.question is not None and not self.question.strip():
            raise ValueError("Enter a question or use the query builder.")
        return self
