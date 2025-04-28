# src/models/book.py

from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl

# Make sure common model is imported correctly
from src.models.common import BaseModelWithTimestamps


class BookBase(BaseModel):
    """
    Base model for Book data, containing common fields.
    Represents a single work/text found on the source website.
    """

    title_sa: str = Field(
        ..., description="Title of the book in Sanskrit (Devanagari)."
    )
    title_en: str = Field(
        ..., description="Title of the book transliterated to IAST (or English)."
    )
    section: str = Field(..., description="Section/category the book belongs to.")
    order_in_section: int = Field(
        ..., description="Order of appearance within its section."
    )
    source_url: HttpUrl = Field(
        ..., description="The primary URL linking to this book."
    )
    page_urls: List[str] = Field(
        default_factory=list, description="List of discovered page URLs (as strings)."
    )
    sidebar_html: Optional[str] = Field(
        default=None, description="Raw HTML of the sidebar navigation."
    )

    # Phase 1 Statuses
    discovery_status: Optional[str] = Field(
        default="pending", description="Status of initial book link discovery."
    )
    sidebar_status: Optional[str] = Field(
        default="pending", description="Status of sidebar/page list fetching."
    )

    # Phase 2 Statuses & Progress
    sutra_discovery_status: Optional[str] = Field(
        default="pending", description="Status of sutra discovery (Phase 2)."
    )
    content_fetch_status: Optional[str] = Field(
        default="pending", description="Status of content fetching (Phase 2)."
    )
    sutra_discovery_progress: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=100.0,
        description="Sutra discovery progress % (Phase 2).",
    )
    content_fetch_progress: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=100.0,
        description="Content fetch progress % (Phase 2).",
    )
    total_sutras_discovered: Optional[int] = Field(
        default=None, ge=0, description="Count of unique sutras found (Phase 2)."
    )
    sutras_fetched_count: Optional[int] = Field(
        default=None,
        ge=0,
        description="Count of sutras successfully fetched (Phase 2).",
    )
    sutras_failed_count: Optional[int] = Field(
        default=None,
        ge=0,
        description="Count of sutras that failed fetching (Phase 2).",
    )

    model_config = {
        "extra": "ignore",
    }


class BookCreate(BookBase):
    """
    Model used when creating/upserting a Book document during Phase 1 (fetch_book_details).
    Ensures book_id is present and sets statuses relevant to Phase 1 completion.
    """

    book_id: int = Field(
        ..., description="Integer ID used by dvaitavedanta.in for this book."
    )

    # Phase 1 Statuses
    discovery_status: str = "complete"
    sidebar_status: str  # Must be provided ('complete', 'parse_failed', 'fetch_failed')

    # Phase 2 Statuses & Progress Initialization
    sutra_discovery_status: str = "pending"
    content_fetch_status: str = "pending"
    sutra_discovery_progress: float = 0.0  # Initialize progress
    content_fetch_progress: float = 0.0  # Initialize progress
    total_sutras_discovered: int = 0  # Initialize counts
    sutras_fetched_count: int = 0
    sutras_failed_count: int = 0

    page_urls: List[str] = Field(
        default_factory=list, description="List of discovered page URLs (as strings)."
    )


class BookUpdate(BaseModel):
    """
    Model used for updating an existing Book document.
    Relevant for Phase 2 status/progress updates or potential Phase 1 corrections.
    All fields are optional.
    """

    title_sa: Optional[str] = None
    title_en: Optional[str] = None
    section: Optional[str] = None
    order_in_section: Optional[int] = None
    source_url: Optional[HttpUrl] = None
    page_urls: Optional[List[str]] = None
    sidebar_html: Optional[str] = None
    # Phase 1 Statuses
    discovery_status: Optional[str] = None
    sidebar_status: Optional[str] = None
    # Phase 2 Statuses & Progress
    sutra_discovery_status: Optional[str] = None
    content_fetch_status: Optional[str] = None
    sutra_discovery_progress: Optional[float] = None
    content_fetch_progress: Optional[float] = None
    total_sutras_discovered: Optional[int] = None
    sutras_fetched_count: Optional[int] = None
    sutras_failed_count: Optional[int] = None

    model_config = {"extra": "ignore"}


class BookInDB(BookBase, BaseModelWithTimestamps):
    """
    Model representing a Book document as stored in MongoDB.
    Uses 'id' aliased to '_id' as the primary identifier.
    """

    id: int = Field(
        ...,
        description="MongoDB document ID (same as the book's integer ID).",
        alias="_id",
    )

    model_config = {
        "populate_by_name": True,
        "arbitrary_types_allowed": True,
    }
