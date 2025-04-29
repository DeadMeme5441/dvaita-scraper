# src/models/book.py

from typing import List, Optional
from pydantic import BaseModel, Field, HttpUrl

# Make sure common model is imported correctly
from src.models.common import BaseModelWithTimestamps


class BookBase(BaseModel):
    """Base model for Book data, containing common fields."""

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
    discovery_status: Optional[str] = Field(default="pending")
    sidebar_status: Optional[str] = Field(default="pending")
    # Phase 2 Statuses & Progress
    sutra_discovery_status: Optional[str] = Field(default="pending")
    content_fetch_status: Optional[str] = Field(default="pending")
    sutra_discovery_progress: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    content_fetch_progress: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    total_sutras_discovered: Optional[int] = Field(default=None, ge=0)
    sutras_fetched_count: Optional[int] = Field(default=None, ge=0)
    sutras_failed_count: Optional[int] = Field(default=None, ge=0)
    # --- Phase 3 Status ---
    phase3_status: Optional[str] = Field(
        default="pending",
        description="Status of Phase 3 (e.g., pending, preprocessing, preprocessing_complete, generating_markdown, pending_markdown_review, error)",
    )
    markdown_output_path: Optional[str] = Field(
        default=None, description="Path where markdown review files were saved."
    )

    model_config = {"extra": "ignore"}


class BookCreate(BookBase):
    """Model used when creating/upserting a Book document during Phase 1."""

    book_id: int = Field(...)
    discovery_status: str = "complete"
    sidebar_status: str
    sutra_discovery_status: str = "pending"
    content_fetch_status: str = "pending"
    sutra_discovery_progress: float = 0.0
    content_fetch_progress: float = 0.0
    total_sutras_discovered: int = 0
    sutras_fetched_count: int = 0
    sutras_failed_count: int = 0
    phase3_status: str = "pending"  # Initialize Phase 3 status
    markdown_output_path: Optional[str] = None
    page_urls: List[str] = Field(default_factory=list)


class BookUpdate(BaseModel):
    """Model used for updating an existing Book document."""

    title_sa: Optional[str] = None
    title_en: Optional[str] = None
    section: Optional[str] = None
    order_in_section: Optional[int] = None
    source_url: Optional[HttpUrl] = None
    page_urls: Optional[List[str]] = None
    sidebar_html: Optional[str] = None
    discovery_status: Optional[str] = None
    sidebar_status: Optional[str] = None
    sutra_discovery_status: Optional[str] = None
    content_fetch_status: Optional[str] = None
    sutra_discovery_progress: Optional[float] = None
    content_fetch_progress: Optional[float] = None
    total_sutras_discovered: Optional[int] = None
    sutras_fetched_count: Optional[int] = None
    sutras_failed_count: Optional[int] = None
    phase3_status: Optional[str] = None  # Allow updating phase 3 status
    markdown_output_path: Optional[str] = None
    model_config = {"extra": "ignore"}


class BookInDB(BookBase, BaseModelWithTimestamps):
    """Model representing a Book document as stored in MongoDB."""

    id: int = Field(..., alias="_id")
    model_config = {"populate_by_name": True, "arbitrary_types_allowed": True}
