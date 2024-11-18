"""
User data transformer module using Pandas.

This module handles the transformation and validation of user data,
ensuring data quality and standardization.
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from .base_transformer import BaseTransformer, TransformerError

VALID_GENDERS = {
    'Agender', 'Bigender', 'Female', 'Genderfluid',
    'Gender nonconforming', 'Genderqueer', 
    'Gender questioning', 'Male', 'Non-binary'
}

class UsersTransformer(BaseTransformer):
    """
    Transforms user data using Pandas for efficient processing.
    
    This transformer handles validation and standardization of user records,
    including personal information, email uniqueness, and timestamp formatting.
    
    Attributes:
        _required_fields (List[str]): Required fields in user records
    """
    
    def __init__(self):
        """Initialize UsersTransformer with required fields."""
        super().__init__()
        self._required_fields = [
            'id', 'first_name', 'last_name', 'email',
            'gender', 'favorite_genres', 'created_at', 'updated_at'
        ]

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform user DataFrame with validation.
        
        Args:
            df: Input DataFrame with user data
            
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
            df = self._validate_timestamps(df, ['created_at', 'updated_at'])
            df = self._handle_duplicates(df)
            
            # Remove invalid records
            df = df.dropna()
            
            self._log_transformation_summary(df, "users")
            return df
            
        except Exception as e:
            raise TransformerError(f"User transformation failed: {str(e)}") from e

    def _validate_basic_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate and transform basic user fields.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: Validated DataFrame
        """
        # ID validation
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        
        # String fields validation
        string_fields = ['first_name', 'last_name', 'email']
        df = self._validate_string_columns(df, string_fields)
        
        # Email standardization
        df['email'] = df['email'].str.lower()
        
        # Gender validation
        df['gender'] = df['gender'].apply(self._validate_gender)
        
        # Genres parsing
        df['favorite_genres'] = df['favorite_genres'].apply(
            lambda x: x.replace('{', '').replace('}', '').strip() if isinstance(x, str) else None
        )
        
        return df

    def _validate_gender(self, gender: Any) -> Optional[str]:
        """
        Validate gender against allowed values.
        
        Args:
            gender: Gender value to validate
            
        Returns:
            Optional[str]: Validated gender or None if invalid
        """
        if pd.isna(gender) or not isinstance(gender, str):
            return None
            
        gender = gender.strip()
        if gender not in VALID_GENDERS:
            self._validation_errors.append(f"Invalid gender value: {gender}")
            return None
            
        return gender

    def _handle_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle duplicate email entries.
        
        Args:
            df: Input DataFrame
            
        Returns:
            pd.DataFrame: DataFrame with duplicates removed
        """
        duplicates = df[df.duplicated(subset=['email'], keep=False)]
        if not duplicates.empty:
            self._validation_errors.extend(
                f"Duplicate email found: {email}" 
                for email in duplicates['email'].unique()
            )
        return df.drop_duplicates(subset=['email'], keep='first')

    

