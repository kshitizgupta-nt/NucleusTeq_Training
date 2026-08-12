# Member Data Processor Assignment

A small, modular Python utility built for the Core Concepts training assignment. It cleans up a list of raw member profiles, validates data using regex, and packages everything into a distributable wheel file.

## Setup & Installation

1. Set up a virtual environment:
```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
```

2. Build the package:
```powershell
python setup.py sdist bdist_wheel
```

3. Install the generated .whl file:
```powershell
pip install .\dist\data_processor_task-1.0.0-py3-none-any.whl
```

## How It Works

- **`my_processor/utils.py`**: Handles regex data validation patterns (emails/phones) and defines the custom `InvalidMemberDataError` exception.
- **`my_processor/core.py`**: Holds the main `Member` class logic, loads the raw dict data, filters/maps it, and runs the script.
- Uses standard `try-except` blocks to skip bad rows without crashing the whole program.

## Quick Test

```python
from my_processor.core import Member

m = Member("John Doe", "john@example.com", "555-0101")
print(m)
```
