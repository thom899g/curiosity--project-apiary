"""
Firestore Client Singleton - The central nervous system of Project Apiary.
Uses Firebase Admin SDK for Firestore as the immutable coordination ledger.
"""
import os
import json
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

import firebase_admin
from firebase_admin import credentials, firestore
from firebase_admin.exceptions import FirebaseError
from google.cloud.firestore_v1 import Client as FirestoreClient
from google.cloud.firestore_v1.base_query import FieldFilter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

class TaskState(Enum):
    """Immutable task states in the Firestore ledger."""
    PENDING = "pending"
    ASSIGNED = "assigned"
    COMPLETED = "completed"
    FAILED = "failed"
    QUARANTINED = "quarantined"

class TaskPriority(Enum):
    """Task priority levels for intelligent scheduling."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3

@dataclass
class TaskDefinition:
    """Immutable task definition schema for Firestore."""
    task_id: str
    task_type: str
    payload: Dict[str, Any]
    created_at: datetime
    priority: TaskPriority
    state: TaskState
    metadata: Optional[Dict[str, Any]] = None
    max_retries: int = 3
    current_retries: int = 0
    
    def to_firestore(self) -> Dict[str, Any]:
        """Convert to Firestore-serializable format."""
        return {
            'task_id': self.task_id,
            'task_type': self.task_type,
            'payload': self.payload,
            'created_at': self.created_at,
            'priority': self.priority.value,
            'state': self.state.value,
            'metadata': self.metadata or {},
            'max_retries': self.max_retries,
            'current_retries': self.current_retries
        }
    
    @classmethod
    def from_firestore(cls, data: Dict[str, Any]) -> 'TaskDefinition':
        """Create from Firestore document."""
        return cls(
            task_id=data['task_id'],
            task_type=data['task_type'],
            payload=data['payload'],
            created_at=data['created_at'],
            priority=TaskPriority(data['priority']),
            state=TaskState(data['state']),
            metadata=data.get('metadata'),
            max_retries=data.get('max_retries', 3),
            current_retries=data.get('current_retries', 0)
        )

class FirestoreCoordinator:
    """Singleton coordinator for all Firestore operations with retry logic."""
    
    _instance: Optional['FirestoreCoordinator'] = None
    _client: Optional[FirestoreClient] = None
    _initialized: bool = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._initialize_firebase()
            self._initialized = True
    
    def _initialize_firebase(self) -> None:
        """Initialize Firebase Admin SDK with credentials."""
        try:
            cred_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'credentials.json')
            if not os.path.exists(cred_path):
                raise FileNotFoundError(f"Firebase credentials not found at {cred_path}")
            
            # Check if already initialized
            if not firebase_admin._apps:
                cred = credentials.Certificate(cred_path)
                firebase_admin.initialize_app(cred)
            
            self._client = firestore.client()
            logger.info(f"Firestore client initialized for project: {self._client.project}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Firebase: {str(e)}")
            raise
    
    @property
    def client(self) -> FirestoreClient:
        """Get Firestore client with lazy initialization."""
        if self._client is None:
            self._initialize_firebase()
        return self._client
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((FirebaseError, ConnectionError))
    )
    def create_task(self, task_def: TaskDefinition) -> str:
        """Atomically create a new task in Firestore ledger."""
        try:
            task_ref = self.client.collection('tasks').document(task_def.task_id)
            
            # Check for duplicate
            if task_ref.get().exists:
                logger.warning(f"Task {task_def.task_id} already exists")
                return task_def.task_id
            
            task_data = task_def.to_firestore()
            task_ref.set(task_data)
            
            logger.info(f"Created task {task_def.task_id} of type {task_def.task_type}")
            return task_def.task_id
            
        except FirebaseError as e:
            logger.error(f"Firestore error creating task {task_def.task_id}: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error creating task: {str(e)}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def claim_task(self, bee_id: str, task_type: str) -> Optional[TaskDefinition]:
        """
        Atomic task claiming using Firestore transactions.
        Implements pull-based work distribution pattern.
        """
        @firestore.transactional
        def claim_in_transaction(transaction, tasks_ref, assignments_ref):
            # Query for pending task of specified type, ordered by priority and creation time
            query = (tasks_ref
                    .where(filter=FieldFilter('state', '==', TaskState.PENDING.value))
                    .where(filter=FieldFilter('task_type', '==', task_type))
                    .order_by('priority')
                    .order_by('created_at')
                    .limit(1))
            
            docs = list(query.get(transaction=transaction))
            if not docs:
                return None
            
            task_doc = docs[0]
            task_id = task_doc.id
            
            # Check if already assigned
            assignment_ref = assignments_ref.document(f"{bee_id}_{task_id}")
            if assignment_ref.get(transaction=transaction).exists:
                return None
            
            # Create assignment (lock)
            assignment_data = {
                'bee_id': bee_id,
                'task_id': task_id,
                'assigned_at': datetime.utcnow(),
                'task_type': task_type
            }
            transaction.set(assignment_ref, assignment_data)
            
            # Update task state
            transaction.update(task_doc.reference, {
                'state': TaskState.ASSIGNED.value,
                'assigned_at': datetime.utcnow(),
                'assigned_to': bee_id
            })
            
            return TaskDefinition.from_firestore(task_doc.to_dict())
        
        try:
            transaction = self.client.transaction()
            task = claim_in_transaction(
                transaction,
                self.client.collection('tasks'),
                self.client.collection('assignments')
            )
            
            if task:
                logger.info(f"Bee {bee_id} claimed task {task.task_id}")
            return task
            
        except FirebaseError as e:
            logger.error(f"Firestore transaction failed for bee {bee_id}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in claim_task: {str(e)}")
            return None
    
    @retry(stop=stop_after_attempt(3))
    def complete_task(self, bee_id: str, task_id: str, 
                     result: Dict[str, Any], profit_loss: float = 0.0) -> bool:
        """
        Complete a task atomically and record results.
        Implements effective-once semantics with result logging.
        """
        @firestore.transactional
        def complete_in_transaction(transaction):
            # Get assignment
            assignment_ref = self.client.collection('assignments').document(f"{bee_id}_{task_id}")
            assignment = assignment_ref.get(transaction=transaction)
            
            if not assignment.exists:
                logger.warning(f"Assignment not found for bee {bee_id}, task {task_id}")
                return False
            
            # Get task
            task_ref = self.client.collection('tasks').document(task_id)
            task = task_ref.get(transaction=transaction)
            
            if not task.exists:
                logger.error(f"Task {task_id} not found")
                transaction.delete(assignment_ref)
                return False
            
            # Update task state
            transaction.update(task_ref, {
                'state': TaskState.COMPLETED.value,
                'completed_at': datetime.utcnow(),
                'profit_loss': profit_loss
            })
            
            # Add result to subcollection
            result_ref = task_ref.collection('results').document()
            result_data = {
                'result_id': result_ref.id,
                'bee_id': bee_id,
                'executed_at': datetime.utcnow(),
                'result': result,
                'profit_loss': profit_loss,
                'success': True
            }
            transaction.set(result_ref, result_data)
            
            # Delete assignment
            transaction.delete(assignment_ref)
            
            return True
        
        try:
            transaction = self.client.transaction()
            success = complete_in_transaction(transaction)
            
            if success:
                logger.info(f"Bee {bee_id} completed task {task_id} with profit: ${profit_loss:.6f}")
            return success
            
        except FirebaseError as e:
            logger.error(f"Firestore error completing task {task_id}: {str(e)}")
            return False
    
    @retry(stop=stop_after_attempt(3))
    def fail_task(self, bee_id: str, task_id: str, 
                 error: str, max_retries_exceeded: bool = False) -> bool:
        """Mark a task as failed with automatic retry logic."""
        @firestore.transactional
        def fail_in_transaction(transaction):
            task_ref = self.client.collection('tasks').document(task_id)
            task = task_ref.get(transaction=transaction)
            
            if not task.exists:
                return False
            
            task_data = task.to_dict()
            current_retries = task_data.get('current_retries', 0)
            max_retries = task_data.get('max_retries', 3)
            
            # Check if should retry or quarantine
            if max_retries_exceeded or current_retries >= max_retries:
                new_state = TaskState.QUARANTINED.value
                logger.warning(f"Task {task_id} quarantined after {current_retries} retries")
            else:
                new_state = TaskState.PENDING.value
                logger.info(f"Task {task_id} failed, will retry ({current_retries + 1}/{max_retries})")
            
            # Update task
            updates = {
                'state': new_state,
                'current_retries': current_retries + 1,
                'last_error': error,
                'failed_at': datetime.utcnow()
            }
            
            if new_state == TaskState.PENDING.value:
                updates['assigned_at'] = firestore.DELETE_FIELD
                updates['assigned_to'] = firestore.DELETE_FIELD
            
            transaction.update(task_ref, updates)
            
            # Delete assignment if exists
            assignment_ref = self.client.collection('assignments').document(f"{bee_id}_{task_id}")
            if assignment_ref.get(