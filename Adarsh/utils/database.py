import datetime
import motor.motor_asyncio
import secrets
import time


class Database:
    def __init__(self, uri, database_name):
        self.enabled = bool(uri and uri.strip())
        if self.enabled:
            self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
            self.db = self._client[database_name]
            self.col = self.db.users
            self.temp_files = self.db.temp_files
        else:
            self._client = None
            self.db = None
            self.col = None
            self.temp_files = None
            # In-memory storage for development/testing
            self._memory_temp_files = {}
            print("Database disabled - no DATABASE_URL provided, using in-memory storage")

    def new_user(self, id):
        return dict(
            id=id,
            join_date=datetime.date.today().isoformat()
        )

    async def add_user(self, id):
        if not self.enabled:
            return
        user = self.new_user(id)
        await self.col.insert_one(user)
        
    async def add_user_pass(self, id, ag_pass):
        if not self.enabled:
            return
        await self.add_user(int(id))
        await self.col.update_one({'id': int(id)}, {'$set': {'ag_p': ag_pass}})
    
    async def get_user_pass(self, id):
        if not self.enabled:
            return None
        user_pass = await self.col.find_one({'id': int(id)})
        return user_pass.get("ag_p", None) if user_pass else None
    
    async def is_user_exist(self, id):
        if not self.enabled:
            return False
        user = await self.col.find_one({'id': int(id)})
        return True if user else False

    async def total_users_count(self):
        if not self.enabled:
            return 0
        count = await self.col.count_documents({})
        return count

    async def get_all_users(self):
        if not self.enabled:
            return []
        all_users = self.col.find({})
        return all_users

    async def delete_user(self, user_id):
        if not self.enabled:
            return
        await self.col.delete_many({'id': int(user_id)})

    # Temporary file storage for intermediate page system
    async def store_temp_file(self, message_data):
        """Store temporary file data and return a unique token"""
        token = secrets.token_urlsafe(16)
        
        if not self.enabled:
            # Store in memory for testing
            temp_data = {
                'token': token,
                'message_id': message_data['message_id'],
                'file_name': message_data['file_name'], 
                'file_size': message_data['file_size'],
                'mime_type': message_data['mime_type'],
                'caption': message_data['caption'],
                'from_chat_id': message_data['from_chat_id'],
                'file_unique_id': message_data['file_unique_id'],
                'created_at': time.time(),
                'expires_at': time.time() + (24 * 60 * 60)  # 24 hours
            }
            self._memory_temp_files[token] = temp_data
            return token
        # Database enabled - use MongoDB
        temp_data = {
            'token': token,
            'message_id': message_data['message_id'],
            'file_name': message_data['file_name'], 
            'file_size': message_data['file_size'],
            'mime_type': message_data['mime_type'],
            'caption': message_data['caption'],
            'from_chat_id': message_data['from_chat_id'],
            'file_unique_id': message_data['file_unique_id'],
            'created_at': time.time(),
            'expires_at': time.time() + (24 * 60 * 60)  # 24 hours
        }
        await self.temp_files.insert_one(temp_data)
        return token

    async def get_temp_file(self, token):
        """Retrieve temporary file data by token"""
        if not self.enabled:
            # Get from memory storage
            temp_data = self._memory_temp_files.get(token)
            if temp_data and temp_data.get('expires_at', 0) > time.time():
                return temp_data
            elif temp_data:
                # Clean up expired token
                del self._memory_temp_files[token]
            return None
            
        # Get from database
        temp_data = await self.temp_files.find_one({'token': token})
        if temp_data and temp_data.get('expires_at', 0) > time.time():
            return temp_data
        elif temp_data:
            # Clean up expired token
            await self.temp_files.delete_one({'token': token})
        return None

    async def delete_temp_file(self, token):
        """Delete temporary file data after stream generation"""
        if not self.enabled:
            # Delete from memory storage
            if token in self._memory_temp_files:
                del self._memory_temp_files[token]
            return
        await self.temp_files.delete_one({'token': token})

    async def cleanup_expired_temp_files(self):
        """Clean up expired temporary files"""
        if not self.enabled:
            return
        current_time = time.time()
        await self.temp_files.delete_many({'expires_at': {'$lt': current_time}})
