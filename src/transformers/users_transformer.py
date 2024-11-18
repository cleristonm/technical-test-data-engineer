from .base_transformer import BaseTransformer
from typing import List, Dict, Any

class UsersTransformer(BaseTransformer):
    """Transforms user data into a standardized format."""
    
    def transform(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transforms raw user data into standardized records.
        
        Args:
            raw_data: List of user records containing personal information
            
        Returns:
            List of transformed user records
        """
        self.log.info(f"Starting user data transformation. Records count: {len(raw_data)}")
        self._validate_input_type(raw_data, "users")
            
        transformed_users = []
        validation_errors = []
        processed_count = 0
        error_count = 0
        processed_emails = set()
        
        for index, user in enumerate(raw_data):
            try:
                self._validate_required_fields(user, [
                    'id', 'first_name', 'last_name', 'email', 
                    'gender', 'favorite_genres', 'created_at', 'updated_at'
                ])
                
                if not isinstance(user['id'], int):
                    raise ValueError(f"Invalid id format: {user['id']}")
                
                email = self._validate_string(user['email'], 'email').lower()
                if email in processed_emails:
                    error_msg = f"Skipping duplicate email: {email}"
                    self.log.warning(error_msg)
                    validation_errors.append(error_msg)
                    error_count += 1
                    continue
                
                transformed_user = {
                    'id': user['id'],
                    'first_name': self._validate_string(user['first_name'], 'first_name'),
                    'last_name': self._validate_string(user['last_name'], 'last_name'),
                    'email': email,
                    'gender': self._validate_gender(user['gender']),
                    'favorite_genres': self._parse_genres(user['favorite_genres']),
                    'created_at': self._validate_timestamp(user['created_at'], 'created_at', validation_errors),
                    'updated_at': self._validate_timestamp(user['updated_at'], 'updated_at', validation_errors)
                }
                
                if all(transformed_user.values()):
                    transformed_users.append(transformed_user)
                    processed_emails.add(email)
                    processed_count += 1
                else:
                    error_count += 1
                
            except (KeyError, ValueError) as e:
                error_msg = f"Error processing user at index {index}: {str(e)}"
                self.log.error(error_msg)
                validation_errors.append(error_msg)
                error_count += 1
                
            except Exception as e:
                error_msg = f"Unexpected error processing user at index {index}: {str(e)}"
                self.log.error(error_msg)
                self.log.error(f"User data: {user}")
                raise Exception(error_msg) from e
        
        self._log_transformation_summary(
            len(raw_data),
            processed_count,
            error_count,
            validation_errors,
            "users"
        )
        
        return transformed_users

    def _validate_gender(self, gender: str) -> str:
        """
        Validates and standardizes gender value.
        
        Args:
            gender: Gender value to validate ('M' or 'F')
            
        Returns:
            Standardized gender value in uppercase
            
        Raises:
            ValueError: If gender value is invalid
        """
        gender = self._validate_string(gender, 'gender')
        if gender.upper() not in ['M', 'F']:
            raise ValueError(f"Invalid gender value: {gender}")
        return gender.upper()