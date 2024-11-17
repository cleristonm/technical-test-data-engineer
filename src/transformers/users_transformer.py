from datetime import datetime
from typing import List, Dict, Any, Set
from src.transformers.base_transformer import BaseTransformer
from airflow.utils.log.logging_mixin import LoggingMixin

class UsersTransformer(BaseTransformer, LoggingMixin):
    def __init__(self):
        super().__init__()
        self.processed_emails: Set[str] = set()  # Set to track processed emails

    def transform(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Transform raw user data into the desired format, skipping duplicate emails.
        
        Args:
            raw_data (Dict[str, Any]): Raw data containing user information
            
        Returns:
            List[Dict[str, Any]]: List of transformed user records without email duplicates
        """
        self.log.info("Starting user data transformation")
        transformed_users = []
        
        try:
            self.log.info(f"Processing {len(raw_data)} user records")
            duplicates_count = 0
            
            for user in raw_data:
                email = user['email'].lower()
                
                # Skip if email is already processed
                if email in self.processed_emails:
                    self.log.warning(f"Skipping duplicate email: {email}")
                    duplicates_count += 1
                    continue
                
                transformed_user = {
                    'id': user['id'],
                    'first_name': user['first_name'],
                    'last_name': user['last_name'],
                    'email': email,
                    'gender': user['gender'],
                    'favorite_genres': user['favorite_genres'].split(',') if ',' in user['favorite_genres'] else [user['favorite_genres']],
                    'created_at': datetime.fromisoformat(user['created_at']).isoformat(),
                    'updated_at': datetime.fromisoformat(user['updated_at']).isoformat()
                }
                
                self.processed_emails.add(email)
                transformed_users.append(transformed_user)
                
            self.log.info(f"User transformation completed. Processed: {len(transformed_users)}, Skipped duplicates: {duplicates_count}")
            return transformed_users
            
        except KeyError as e:
            error_msg = f"Missing required field in user data: {str(e)}"
            self.log.error(error_msg)
            raise ValueError(error_msg)
            
        except ValueError as e:
            error_msg = f"Invalid date format in user data: {str(e)}"
            self.log.error(error_msg)
            raise ValueError(error_msg)
            
        except Exception as e:
            error_msg = f"Unexpected error during user transformation: {str(e)}"
            self.log.error(error_msg)
            raise Exception(error_msg)

    def reset_processed_emails(self):
        """Reset the set of processed emails"""
        self.processed_emails.clear()