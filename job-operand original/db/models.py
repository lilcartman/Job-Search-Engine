"""
SQLAlchemy models for the openjobdata dataset.

The `Company` schema below matches the declarative mapping published in the
openjobdata documentation (https://openjobdata.com/documentation). The `Job`
schema is inferred from the dataset's parquet columns for the `minimal`
variant (job-level fields, no long-form JSON columns) plus the documented
foreign key relationship to `companies`. If the live dataset has additional
or renamed columns when you inspect it (see etl/download.py, which prints
the schema), update this file to match before running init_db.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    func,
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()


class Company(Base):
    __tablename__ = "companies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    unique_id = Column(String, unique=True, index=True)
    career_url = Column(String, nullable=True)
    region = Column(String, nullable=True)
    country = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    linkedin_url = Column(String, nullable=True)

    jobs = relationship("Job", back_populates="company")


class Job(Base):
    """
    Columns match the actual 'minimal' variant parquet schema (verified by
    inspecting etl.download output), not the openjobdata docs' full field
    list — salary/seniority/locality/region are not present in this variant
    and are intentionally omitted.
    """

    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    unique_id = Column(String, unique=True, index=True)  # dedupe key from source (parquet's 'id')
    job_id = Column(String, nullable=True)  # source ATS's raw job id
    company_id = Column(Integer, ForeignKey("companies.id"), index=True)

    title = Column(String, index=True)
    department = Column(String, nullable=True)
    country = Column(String, nullable=True)
    # From the full variant's job_model_json.location.city, fetched only for
    # matched rows (etl.sync.add_city_from_full). Upstream values are dirty:
    # sometimes a state name or a semicolon list of states instead of a city.
    city = Column(String, nullable=True)
    remote = Column(Boolean, nullable=True)  # parquet: is_remote

    employment_type = Column(String, nullable=True)  # full-time / contract / etc.
    workplace_type = Column(String, nullable=True)  # e.g. remote / onsite / hybrid / tbc

    apply_url = Column(String, nullable=True)

    status = Column(String, nullable=True, index=True)  # e.g. open / closed
    posted_at = Column(DateTime, nullable=True)
    fetched_time = Column(DateTime, nullable=True)  # source's own last-fetched timestamp
    closed_at = Column(DateTime, nullable=True, index=True)  # parquet: close_time

    # Populated later, only for jobs that match our target-role filters,
    # via etl/fetch_full_for_matches.py
    description = Column(String, nullable=True)

    synced_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    company = relationship("Company", back_populates="jobs")

    __table_args__ = (
        Index("ix_jobs_title_status", "title", "status"),
    )
