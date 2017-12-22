#pragma once

#include <string>
#include <vector>
#include <cstdint>

// please call load_config to load a configuration from a file at the
// beginning of the process, otherwise none of the dsn_config_xxx functions works
bool load_config(const char *file, const char *arguments);

const char *
dsn_config_get_value_string(const char *section,       ///< [section]
                            const char *key,           ///< key = value
                            const char *default_value, ///< if [section] key is not present
                            const char *dsptr          ///< what it is for, as help-info in config
                            );

bool dsn_config_get_value_bool(const char *section,
                               const char *key,
                               bool default_value,
                               const char *dsptr);

uint64_t dsn_config_get_value_uint64(const char *section,
                                     const char *key,
                                     uint64_t default_value,
                                     const char *dsptr);

double dsn_config_get_value_double(const char *section,
                                   const char *key,
                                   double default_value,
                                   const char *dsptr);

void dsn_config_get_all_sections(/*out*/ std::vector<std::string> &sections);

// return the name of all sections
void dsn_config_get_all_sections(/*out*/ std::vector<const char *> &sections);

// return all keys in some specific section
void dsn_config_get_all_keys(const char *section, /*out*/ std::vector<const char *> &keys);

void dsn_config_dump(std::ostream &os);

void dsn_config_dump_to_file(const char *fname);

void dsn_config_set(const char *section, const char *key, const char *value, const char *dsptr);
