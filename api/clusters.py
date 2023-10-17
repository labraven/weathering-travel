

import os
import time

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


# create a cluster
latest = w.clusters.select_spark_version(latest=True)

cluster_name = f'sdk-{time.time_ns()}'

clstr = w.clusters.create(cluster_name=cluster_name,
                          spark_version=latest,
                          node_type_id='Standard_DS3_v2',
                          autotermination_minutes=15,
                          num_workers=1).result()

# cleanup
# w.clusters.permanent_delete(cluster_id=clstr.cluster_id)

# update a cluster