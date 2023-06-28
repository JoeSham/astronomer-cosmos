"Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account file."
from __future__ import annotations

from typing import Any

from cosmos.profiles.base import BaseProfileMapping


class GoogleCloudServiceAccountFileProfileMapping(BaseProfileMapping):
    """
    Maps Airflow GCP connections to dbt BigQuery profiles if they use a service account file.

    https://docs.getdbt.com/reference/warehouse-setups/bigquery-setup#service-account-file
    https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
    """

    airflow_connection_type: str = "google_cloud_platform"

    required_fields = [
        "project",
        "dataset",
        # plus one of: keyfile or keyfile_dict
    ]

    airflow_param_mapping = {
        "project": "extra.project",
        "dataset": "dataset",
        # multiple options for keyfile/keyfile_dict param name because of older Airflow versions
        "keyfile": ["key_path", "extra__google_cloud_platform__key_path", "extra.key_path"],
        "keyfile_dict": ["keyfile_dict", "extra__google_cloud_platform__keyfile_dict", "extra.keyfile_dict"],
    }

    @property
    def profile(self) -> dict[str, Any | None]:
        """
            Generates profile. Defaults `threads` to 1.
            Profile can either use keyfile as path to json file, or keyfile_dict as directly the json dict
        """

        profile_dict = {
            "type": "bigquery",
            "project": self.project,
            "dataset": self.dataset,
            "threads": self.profile_args.get("threads") or 1,
            **self.profile_args,
        }

        # use keyfile_dict if it exists, otherwise use keyfile
        try:
            profile_dict["keyfile_json"] = self.keyfile_dict
            profile_dict["method"] = "service-account-json"
        except AttributeError:
            profile_dict["keyfile"] = self.keyfile
            profile_dict["method"] = "service-account"

        
        return profile_dict
    
