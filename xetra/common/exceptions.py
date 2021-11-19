"""
Used for creating custom exceptions if needed
"""

class WrongFormatException(Exception):
    """
    This class is used as a wrapper to raise 
    exceptions based off wrong file types
    """
    
class WrongMetaFileException(Exception):
    """
    This class is used as a wrapper to raise
    exceptions based off meta file data
    """
    