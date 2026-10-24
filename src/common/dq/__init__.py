"""Engine-agnostic data-quality framework.

    rules/*.yml   WHAT good data means (written once, owned with the business)
    dqx_engine    HOW to check it with Databricks Labs DQX
    gx_engine     HOW to check it with Great Expectations (GX Core)
    runner        ONE entry point used by every Gold job: run_gate(...)
"""
