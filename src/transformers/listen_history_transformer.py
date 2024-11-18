"""
Listen history transformer module using Pandas.

This module handles the transformation and validation of user listening history data,
ensuring data quality and standardization.
"""

import pandas as pd
from .base_transformer import BaseTransformer, TransformerError


class ListenHistoryTransformer(BaseTransformer):
    """
    Transforms user listening history data using Pandas for efficient processing.

    This transformer handles validation and standardization of listening history records,
    including user validation, track listing expansion, and timestamp formatting.

    Attributes:
        _required_fields (List[str]): Required fields in history records
    """

    def __init__(self):
        """Initialize ListenHistoryTransformer with required fields."""
        super().__init__()
        self._required_fields = ["user_id", "items", "created_at", "updated_at"]

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform listening history DataFrame with validation.

        Args:
            df: Input DataFrame with listening history data

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

            # Validate basic fields
            df = self._validate_basic_fields(df)

            # Expand track listings
            df = self._expand_track_listings(df)

            # Validate timestamps
            df = self._validate_timestamps(df, ["created_at", "updated_at"])

            # Remove invalid records
            df = df.dropna()

            self._log_transformation_summary(df, "listen histories")
            return df

        except Exception as e:
            raise TransformerError(
                f"Listen history transformation failed: {str(e)}"
            ) from e

    def _validate_basic_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate user IDs and basic fields.

        Args:
            df: Input DataFrame

        Returns:
            pd.DataFrame: Validated DataFrame
        """
        # Validate user_id
        df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")

        # Validate items field is list
        invalid_items = ~df["items"].apply(lambda x: isinstance(x, list))
        if invalid_items.any():
            self._validation_errors.extend(
                f"Invalid items format at index {idx}: {val}"
                for idx, val in df[invalid_items]["items"].items()
            )
            df.loc[invalid_items, "items"] = None

        return df

    def _expand_track_listings(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Expand track listings into individual user-track records.

        Args:
            df: DataFrame with columns
                [user_id, items (list), created_at, updated_at]

        Returns:
            pd.DataFrame: Expanded DataFrame with columns
                [user_id, track_id, created_at, updated_at]

        Example:
            Input DataFrame:
                user_id  items           created_at  updated_at
                1       [101, 102, 103]  2024-01-01  2024-01-01
                1       101       2024-01-01  2024-01-01
                1       102       2024-01-01  2024-01-01
                1       103       2024-01-01  2024-01-01

            Output DataFrame:
                user_id  track_id  created_at  updated_at
                1       101       2024-01-01  2024-01-01
                1       102       2024-01-01  2024-01-01
                1       103       2024-01-01  2024-01-01
        """
        # Filter valid records
        valid_mask = df["user_id"].notna() & df["items"].apply(
            lambda x: isinstance(x, list) and len(x) > 0
        )

        if not valid_mask.any():
            return pd.DataFrame(
                columns=["user_id", "track_id", "created_at", "updated_at"]
            )

        # Explode the items list into separate rows
        expanded_df = (
            df[valid_mask].explode("items").rename(columns={"items": "track_id"})
        )

        # Validate track_ids
        expanded_df["track_id"] = pd.to_numeric(
            expanded_df["track_id"], errors="coerce"
        )
        valid_tracks = expanded_df["track_id"].notna() & (expanded_df["track_id"] >= 0)

        if not valid_tracks.all():
            self._validation_errors.extend(
                f"Invalid track_id for user {user_id}: {track_id}"
                for user_id, track_id in expanded_df[~valid_tracks][
                    ["user_id", "track_id"]
                ].values
            )

        return expanded_df[valid_tracks][
            ["user_id", "track_id", "created_at", "updated_at"]
        ]
