"""
Track data transformer module using Pandas.

This module handles the transformation and validation of music track data,
ensuring data quality and standardization.
"""

import pandas as pd
from .base_transformer import BaseTransformer, TransformerError


class TracksTransformer(BaseTransformer):
    """
    Transforms music track data using Pandas for efficient processing.

    This transformer handles validation and standardization of track records,
    including duration formatting, genre parsing, and timestamp validation.

    Attributes:
        _required_fields (List[str]): Required fields in track records
    """

    def __init__(self):
        """Initialize TracksTransformer with required fields."""
        super().__init__()
        self._required_fields = [
            "id",
            "name",
            "artist",
            "duration",
            "genres",
            "created_at",
            "updated_at",
        ]

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform track DataFrame with validation.

        Args:
            df: Input DataFrame with track data

        Returns:
            pd.DataFrame: Transformed and validated DataFrame

        Raises:
            TransformerError: If required fields are missing or validation fails
        """
        try:
            # Validate required fields
            missing_fields = set(self._required_fields) - set(df.columns)
            if missing_fields:
                raise TransformerError(f"Missing required fields: {missing_fields}")

            # Validate and transform fields
            df = self._validate_basic_fields(df)
            df = self._validate_timestamps(df, ["created_at", "updated_at"])
            df = self._validate_duration(df)
            df = self._transform_genres(df)

            # Remove invalid records
            df = df.dropna()

            self._log_transformation_summary(df, "tracks")
            return df

        except Exception as e:
            raise TransformerError(f"Track transformation failed: {str(e)}") from e

    def _validate_basic_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and transform basic track fields.

        Args:
            df: Input DataFrame

        Returns:
            pd.DataFrame: Validated DataFrame
        """
        # ID validation
        df["id"] = pd.to_numeric(df["id"], errors="coerce")

        # String fields validation
        string_fields = ["name", "artist"]
        df = self._validate_string_columns(df, string_fields)

        return df

    def _validate_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and standardize duration format (MM:SS).

        Args:
            df: Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with validated durations
        """

        def is_valid_duration(duration: str) -> bool:
            """Check if duration string matches MM:SS format."""
            if not isinstance(duration, str) or ":" not in duration:
                return False
            try:
                minutes, seconds = duration.split(":")
                return (
                    minutes.isdigit()
                    and seconds.isdigit()
                    and 0 <= int(minutes)
                    and 0 <= int(seconds) <= 59
                )
            except ValueError:
                return False

        # Apply validation
        invalid_durations = ~df["duration"].apply(is_valid_duration)
        if invalid_durations.any():
            self._validation_errors.extend(
                f"Invalid duration format at index {idx}: {val}"
                for idx, val in df[invalid_durations]["duration"].items()
            )
            df.loc[invalid_durations, "duration"] = None

        return df

    def _transform_genres(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and validate genre listings.

        Args:
            df: Input DataFrame

        Returns:
            pd.DataFrame: DataFrame with transformed genres
        """

        def clean_genres(genres: str) -> str:
            """Clean and validate genre string."""
            if not isinstance(genres, str):
                return None
            return genres.replace("{", "").replace("}", "").strip()

        df["genres"] = df["genres"].apply(clean_genres)

        # Validate non-empty genres
        invalid_genres = df["genres"].isna() | (df["genres"] == "")
        if invalid_genres.any():
            self._validation_errors.extend(
                f"Invalid genres at index {idx}" for idx in df[invalid_genres].index
            )

        return df
