# src/models/sutra.py

from typing import Optional
from pydantic import Field, HttpUrl, BaseModel

from src.models.common import BaseModelWithTimestamps


class SutraBase(BaseModel):
    """
    Base model for Sutra data.
    Represents a specific content unit (identified by sutra_id) within a Book,
    often corresponding to a unique API endpoint for fetching content.
    """

    # Identifier from the source website's API/HTML structure
    sutra_id: int = Field(
        ...,
        description="Integer ID used by dvaitavedanta.in for this specific content unit (sutra/page fragment).",
    )
    # Foreign key linking back to the Book
    book_id: int = Field(..., description="The book_id this sutra belongs to.")
    # The specific page URL where this sutra_id was found/listed
    source_page_url: str = Field(
        ..., description="The URL of the page listing this sutra_id."
    )
    # Raw HTML content fetched for this specific sutra_id
    raw_html: Optional[str] = Field(
        default=None,
        description="Raw HTML content fetched from the load-data endpoint for this sutra.",
    )
    # Additional HTML fragment sometimes returned by the API
    tag_html: Optional[str] = Field(
        default=None,
        description="Optional 'tag' HTML fragment returned by the load-data endpoint.",
    )
    # Status tracking
    fetch_status: str = Field(
        default="pending",
        description="Status of fetching raw_html (e.g., pending, complete, failed).",
    )

    model_config = {
        "extra": "ignore",
    }


class SutraCreate(SutraBase):
    """
    Model used when creating a new Sutra document.
    """

    pass  # No additional fields needed for creation


class SutraUpdate(BaseModel):
    """
    Model used for updating an existing Sutra document.
    All fields are optional.
    """

    raw_html: Optional[str] = None
    tag_html: Optional[str] = None
    fetch_status: Optional[str] = None

    model_config = {
        "extra": "ignore",
    }


class SutraInDB(SutraBase, BaseModelWithTimestamps):
    """
    Model representing a Sutra document as stored in MongoDB.
    Includes database ID and timestamps.
    Uses a composite key (_id) based on book_id and sutra_id for uniqueness.
    """

    # Define a composite ID for MongoDB using a sub-document
    # This ensures uniqueness based on the combination of book and sutra.
    id: dict = Field(
        ...,
        description="Composite MongoDB document ID {'book_id': ..., 'sutra_id': ...}.",
        alias="_id",
    )

    model_config = {
        "populate_by_name": True,  # Allow using alias '_id'
        "arbitrary_types_allowed": True,  # Allow dict as ID type
    }
