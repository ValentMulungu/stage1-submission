
import sys
import traceback

from pipeline.ingest import run_ingestion
from pipeline.transform import run_transformation
from pipeline.provision import run_provisioning


def main() -> int:
    try:
        print("[1/3] Bronze ingestion starting...")
        run_ingestion()
        print("[1/3] Bronze ingestion completed.")

        print("[2/3] Silver transformation starting...")
        run_transformation()
        print("[2/3] Silver transformation completed.")

        print("[3/3] Gold provisioning starting...")
        run_provisioning()
        print("[3/3] Gold provisioning completed.")
        return 0
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
