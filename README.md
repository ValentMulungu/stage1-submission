# Nedbank Data Engineering Challenge - Stage 1 Submission

## Submission Overview

This is my Stage 1 submission for the Nedbank Data Engineering Challenge.

The purpose of this submission is to demonstrate a complete, containerised data engineering pipeline that can ingest source data, apply structured transformations, provision curated outputs, and validate the generated datasets.

I designed the solution to run end-to-end through Docker so that the pipeline can be executed in a clean and repeatable environment without depending on my local machine setup.

---

## Project Structure

The submission is structured as follows:

```text
stage1-submission/
│
├── config/
│   └── pipeline_config.yaml
│
├── pipeline/
│   ├── __init__.py
│   ├── ingest.py
│   ├── provision.py
│   ├── run_all.py
│   ├── schemas.py
│   ├── transform.py
│   ├── utils.py
│   └── validate_outputs.py
│
├── .gitignore
├── Dockerfile
├── README.md
└── requirements.txt
```

---

## Folder and File Purpose

### `config/`

This folder contains the pipeline configuration file.

#### `pipeline_config.yaml`

This file controls the main pipeline settings, including input paths, output paths, and other configuration values used by the pipeline.

I kept configuration separate from the code so that the pipeline can be adjusted without changing the Python scripts directly.

---

### `pipeline/`

This folder contains the Python source code for the Stage 1 pipeline.

#### `__init__.py`

This file marks the `pipeline` folder as a Python package so that the project can be executed using:

```bash
python -m pipeline.run_all
```

#### `ingest.py`

This script handles the ingestion layer.

It reads the raw source files from the configured input location, applies basic standardisation, and prepares the data for downstream processing.

#### `schemas.py`

This script contains the expected schema definitions used by the pipeline.

I used this file to keep schema logic centralised and consistent across the pipeline.

#### `transform.py`

This script contains the transformation logic.

It applies the required business and data cleaning rules to prepare structured datasets for final output.

#### `provision.py`

This script handles the provisioning layer.

It writes the final curated outputs into the required output folders.

#### `utils.py`

This script contains reusable helper functions used across the pipeline.

These include common functionality such as path handling, safe file operations, logging support, and other repeated logic.

#### `validate_outputs.py`

This script validates that the required outputs have been created successfully.

It performs checks such as output existence, row-count validation, and basic quality checks depending on the generated output.

#### `run_all.py`

This is the main entry point for the pipeline.

It runs the full Stage 1 process in the correct order:

```text
1. Load configuration
2. Run ingestion
3. Run transformation
4. Run provisioning
5. Run output validation
```

The Docker container also uses this file as the main execution command.

---

## Docker Execution

This submission is designed to run inside Docker.

The Dockerfile defines the runtime environment and executes the pipeline using:

```bash
python -m pipeline.run_all
```

This ensures that the pipeline can run consistently in the challenge environment.

---

## Build Instructions

From the root of the Stage 1 submission folder, run:

```bash
docker build -t nedbank-stage1-submission .
```

---

## Run Instructions

After the Docker image has been built, run the pipeline using:

```bash
docker run --rm nedbank-stage1-submission
```

If the challenge data is mounted from a local folder, run it using a volume mount.

Example:

```bash
docker run --rm ^
  -v "%cd%\data:/data" ^
  nedbank-stage1-submission
```

For PowerShell, the command can also be run as:

```powershell
docker run --rm `
  -v "${PWD}\data:/data" `
  nedbank-stage1-submission
```

---

## Expected Runtime Flow

When the container runs, the pipeline performs the following steps:

1. Reads the configuration from `config/pipeline_config.yaml`
2. Loads the required raw input data
3. Applies schema validation and standardisation
4. Executes transformation logic
5. Writes the final output datasets
6. Runs validation checks on the generated outputs
7. Completes the pipeline with a success or validation message

---

## Output Location

The generated output files are written to the configured output location.

The output path is controlled through:

```text
config/pipeline_config.yaml
```

The pipeline is designed so that output folders are generated at runtime and do not need to be manually created before execution.

---

## Validation

Validation is included as part of the pipeline process.

The validation step checks whether the expected outputs were created correctly and whether the data passed the required quality checks.

The validation logic is handled in:

```text
pipeline/validate_outputs.py
```

This gives confidence that the pipeline did not only run, but also produced usable output.

---

## Requirements

The `requirements.txt` file is included for compatibility with standard Python project structure.

For this submission, I rely mainly on the Python libraries already available in the challenge base image.



---

## How to Review the Submission

To review this submission:

1. Open the root folder.
2. Review `README.md` for instructions.
3. Review `config/pipeline_config.yaml` for pipeline configuration.
4. Review the Python files inside the `pipeline/` folder.
5. Build the Docker image.
6. Run the Docker container.
7. Confirm that the pipeline completes successfully.
8. Confirm that the expected outputs are generated.

---

## Design Approach

My approach for Stage 1 was to keep the solution simple, structured, and repeatable.

I separated the pipeline into clear layers:

```text
Ingestion → Transformation → Provisioning → Validation
```

This makes the code easier to understand, easier to test, and easier to extend in later stages.

I also kept the configuration separate from the pipeline logic so that paths and settings can be changed without rewriting the source code.

---

## Notes

- The pipeline is executed from `pipeline/run_all.py`.
- The project is containerised using Docker.
- The `pipeline` folder is a Python package.
- The `config` folder stores the YAML configuration file.
- Generated outputs should be created during runtime.
- Temporary output folders and generated data should not be committed unless specifically required by the challenge instructions.

---

## Author

Valent Mulungu

Stage 1 Submission  
Nedbank Data Engineering Challenge
