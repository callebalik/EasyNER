# Makefile for EasyNER project
# Provides standardized commands for common tasks
# Platform independent (Windows/Linux/MacOS), uses python and conda commands

# Variables
CONDA_ENV = easyner_env
PYTHON = python
CONFIG = config.json
TEMPLATE = config.template.json
RESULTS_DIR = results
TEST_DIR = tests

.PHONY: all setup env config template validate run run-download run-split run-ner run-analysis clean clean-all test help

# Default target displays help
all: help

# ===== Environment Setup =====
# Setup the conda environment
env:
	@echo "Checking if conda environment $(CONDA_ENV) exists..."
	@conda env list | findstr $(CONDA_ENV) >nul && ( \
		echo "Conda environment $(CONDA_ENV) already exists. Skipping creation." \
	) || ( \
		echo "Creating conda environment from environment.yml..."; \
		conda env create -f environment.yml; \
		echo "Conda environment setup complete." \
	)


# Complete setup (environment + initial config)
setup: env
	@echo "Setting up EasyNER project..."
	@if not exist $(CONFIG) (
		@echo "Config file $(CONFIG) not found. Generating template..."
		$(PYTHON) scripts/config/generator.py --output $(CONFIG)
	) else (
		@echo "Config file $(CONFIG) already exists. Skipping generation."
	)
	@echo "Setup complete. Edit $(CONFIG) with your specific settings."
	@echo "Run 'make validate' to ensure your configuration is valid."

# ===== Configuration Management =====
# Generate template from current config
config-generate:
	@echo "Generating config template from current config..."
	$(PYTHON) scripts/config/generator.py --config $(CONFIG) --output $(TEMPLATE)
	@echo "Template generated: $(TEMPLATE)"

# Validate current config
config-validate:
	@echo "Validating $(CONFIG)..."
	$(PYTHON) scripts/config/validator.py $(CONFIG)

# Edit config with default editor - Windows compatible version
config-edit:
	@echo "Opening $(CONFIG) with default editor..."
	@if defined EDITOR ($(EDITOR) $(CONFIG)) else (notepad $(CONFIG))

# ===== Pipeline Execution =====
# Run the complete pipeline
run:
	@echo "Running EasyNER pipeline with config options defined in $(CONFIG)..."
	$(PYTHON) main.py

# ===== Testing =====
# Run all tests
test:
	@echo "Running all tests..."
	pytest $(TEST_DIR)

# Run specific tests
test-config:
	@echo "Running configuration tests..."
	pytest $(TEST_DIR)/test_config_management.py

# ===== Cleanup =====
# Clean results directories
clean-results:
	@echo "Cleaning results directories..."
	rm -rf $(RESULTS_DIR)/dataloader/* $(RESULTS_DIR)/splitter/* $(RESULTS_DIR)/ner/* $(RESULTS_DIR)/analysis/*
	@echo "Cleaned results directories"

clean-logs:
	@echo "Cleaning logs..."
	rm -rf logs/*
	@echo "Cleaned logs"


	rm -f "timekeep.txt"

# Clean cache (results, cache, etc.)
clean-cache: clean
	@echo "Performing cache cleanup..."
	rm -rf __pycache__/ .pytest_cache/
	@echo "Cache cleanup finished"

# ===== Help =====
help:
	@echo "EasyNER Makefile commands:"
	@echo ""
	@echo "  === Setup ==="
	@echo "  make env	  - Create conda environment from environment.yml"
	@echo "  make setup	- Complete setup (env + initial config)"
	@echo ""
	@echo "  === Configuration ==="
	@echo "  make template	 - Generate template from current config.json"
	@echo "  make validate	 - Validate current config.json"
	@echo "  make edit-config  - Open config.json in default editor"
	@echo ""
	@echo "  === Pipeline Execution ==="
	@echo "  make run	  - Run the complete pipeline"
	@echo "  make run-download - Run only the download/data loading step"
	@echo "  make run-split	- Run only the sentence splitting step"
	@echo "  make run-ner	  - Run only the NER step"
	@echo "  make run-analysis - Run only the analysis step"
	@echo ""
	@echo "  === Testing ==="
	@echo "  make test	 - Run all tests"
	@echo "  make test-config  - Run configuration tests"
	@echo ""
	@echo "  === Cleanup ==="
	@echo "  make clean	- Clean results directories"
	@echo "  make clean-all	- Clean everything (results, cache, etc.)"
	@echo ""