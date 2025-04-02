import os
import pytest
from error_handler import ErrorHandler

os.environ['TEST_MODE'] = 'False'

@pytest.fixture(autouse=True)
def manage_decorator_flag(request):
    if "unit" in request.node.keywords:
        ErrorHandler.DISABLE_DECORATORS = True
    else:
        ErrorHandler.DISABLE_DECORATORS = False