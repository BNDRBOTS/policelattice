from __future__ import annotations

from app.pipeline.acquisition import AutonomousAcquisitionManager


def test_autonomous_acquisition_manager_single():
    manager = AutonomousAcquisitionManager(max_workers=2, request_timeout=3)
    source_def = {
        "id": "mapping_police_violence",
        "name": "Mapping Police Violence",
        "category": "investigative",
        "adapter": "flatfile",
        "access_mode": "file_drop",
        "config": {
            "drop_dir_env": "MANUAL_DROP_DIR",
            "filename_pattern": "mapping_police_violence*.csv",
        },
    }
    result = manager.acquire_source(source_def)
    assert result.status == "success"
    assert result.source_id == "mapping_police_violence"
    assert result.record_count > 0


def test_autonomous_acquisition_manager_all():
    manager = AutonomousAcquisitionManager(max_workers=2, request_timeout=3)
    sources = [
        {
            "id": "mapping_police_violence",
            "adapter": "flatfile",
            "config": {
                "drop_dir_env": "MANUAL_DROP_DIR",
                "filename_pattern": "mapping_police_violence*.csv",
            },
        },
        {
            "id": "abc15_brady_list_database",
            "adapter": "flatfile",
            "config": {
                "drop_dir_env": "MANUAL_DROP_DIR",
                "filename_pattern": "*officer*.csv",
            },
        },
    ]
    results = manager.acquire_all(sources, parallel=True)
    assert len(results) == 2
    assert results["mapping_police_violence"].status == "success"
    assert results["abc15_brady_list_database"].status == "success"
