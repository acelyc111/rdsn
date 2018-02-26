#pragma once
#include <dsn/dist/replication/replication_types.h>
#include <dsn/service_api_cpp.h>
#include <dsn/cpp/serialization.h>

namespace dsn {
namespace replication {
GENERATED_TYPE_SERIALIZATION(mutation_header, THRIFT)
GENERATED_TYPE_SERIALIZATION(mutation_update, THRIFT)
GENERATED_TYPE_SERIALIZATION(mutation_data, THRIFT)
GENERATED_TYPE_SERIALIZATION(replica_configuration, THRIFT)
GENERATED_TYPE_SERIALIZATION(prepare_msg, THRIFT)
GENERATED_TYPE_SERIALIZATION(read_request_header, THRIFT)
GENERATED_TYPE_SERIALIZATION(write_request_header, THRIFT)
GENERATED_TYPE_SERIALIZATION(rw_response_header, THRIFT)
GENERATED_TYPE_SERIALIZATION(prepare_ack, THRIFT)
GENERATED_TYPE_SERIALIZATION(learn_state, THRIFT)
GENERATED_TYPE_SERIALIZATION(learn_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(learn_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(learn_notify_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(group_check_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(group_check_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(node_info, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_update_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_update_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(replica_server_info, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_query_by_node_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_query_by_node_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(create_app_options, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_create_app_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(drop_app_options, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_drop_app_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_list_apps_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_list_nodes_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_cluster_info_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_recall_app_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_create_app_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_meta_control_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_meta_control_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_proposal_action, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_balancer_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_balancer_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_drop_app_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_list_apps_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_list_nodes_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_cluster_info_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_recall_app_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(query_replica_decree_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(query_replica_decree_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(replica_info, THRIFT)
GENERATED_TYPE_SERIALIZATION(query_replica_info_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(query_replica_info_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(query_app_info_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(query_app_info_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_recovery_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_recovery_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(policy_info, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_restore_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(backup_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(backup_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_modify_backup_policy_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_modify_backup_policy_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_add_backup_policy_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_add_backup_policy_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(policy_entry, THRIFT)
GENERATED_TYPE_SERIALIZATION(backup_entry, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_query_backup_policy_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_query_backup_policy_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_report_restore_status_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_report_restore_status_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_query_restore_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(configuration_query_restore_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_add_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_add_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_status_change_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_status_change_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_entry, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_query_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_query_response, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_confirm_entry, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_sync_request, THRIFT)
GENERATED_TYPE_SERIALIZATION(duplication_sync_response, THRIFT)
}
}
