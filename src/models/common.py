# src/models/common.py

from datetime import datetime, timezone
from typing import Optional, Annotated

from pydantic import BaseModel, Field, BeforeValidator
from pydantic_core import core_schema
from bson import ObjectId

# --- Custom ObjectId Type ---
PyObjectId = Annotated[
    ObjectId,
    BeforeValidator(
        lambda v: ObjectId(v) if isinstance(v, str) and ObjectId.is_valid(v) else v
    ),
    core_schema.plain_serializer_function_ser_schema(
        lambda v: str(v),
        when_used="json-unless-none",  # Serialize ObjectId to str for JSON
    ),
]

# --- Base Model ---


class BaseModelWithTimestamps(BaseModel):
    """
    A base Pydantic model including timestamp fields.
    Made optional to handle cases where they might be missing during read,
    although insert/update logic should set them.
    """

    # --- FIX: Made Optional ---
    created_at: Optional[datetime] = Field(
        default=None,  # Allow None during validation
        description="Timestamp when the document was created (UTC).",
        alias="createdAt",
    )
    updated_at: Optional[datetime] = Field(
        default=None,  # Allow None during validation
        description="Timestamp when the document was last updated (UTC).",
        alias="updatedAt",
    )

    model_config = {
        "populate_by_name": True,
        "validate_assignment": True,
        "arbitrary_types_allowed": True,
    }

    # Method to update the updated_at timestamp easily (still useful)
    def touch(self) -> None:
        """Updates the `updated_at` timestamp to the current time in UTC."""
        self.updated_at = datetime.now()
