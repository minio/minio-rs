/*
 * MinIO Rust Library for Amazon S3 Compatible Cloud Storage
 * Copyright 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

pub type Values = HashMap<String, Vec<Option<String>>>;

pub trait ValuesAccess {
    fn get_value(&self, key: &str) -> Option<String>;
    fn set_value(&mut self, key: &str, value: Option<String>);
    fn add_value(&mut self, key: &str, value: Option<String>);
    fn del_value(&mut self, key: &str);
}

impl ValuesAccess for Values {
    /// Gets the first item for a given key. If the key is invalid it returns `None`
    /// To get multiple values use the `Values` instance as map.
    fn get_value(&self, key: &str) -> Option<String> {
        let value_vec = match self.get(key) {
            Some(v) => v,
            None => return None,
        };
        if value_vec.len() == 0 {
            return None;
        }
        return value_vec.get(0).unwrap().clone();
    }

    /// Sets the key to value. It replaces any existing values.
    fn set_value(&mut self, key: &str, value: Option<String>) {
        self.insert(key.to_string(), vec![value]);
    }

    /// Add adds the value to key. It appends to any existing values associated with key.
    fn add_value(&mut self, key: &str, value: Option<String>) {
        match self.get_mut(key) {
            Some(value_vec) => value_vec.push(value),
            None => (),
        }
    }

    // Del deletes the values associated with key.
    fn del_value(&mut self, key: &str) {
        self.remove(key);
    }
}

#[cfg(test)]
mod net_tests {
    use super::*;

    #[test]
    fn values_set() {
        let mut values = Values::new();
        values.set_value("key", Some("value".to_string()));
        assert_eq!(values.len(), 1);
        assert_eq!(values.get("key").unwrap().len(), 1);

        values.set_value("key", None);
        assert_eq!(values.len(), 1);
        assert_eq!(values.get("key").unwrap().len(), 1);
    }

    #[test]
    fn values_add() {
        let mut values = Values::new();
        values.set_value("key", Some("value".to_string()));
        assert_eq!(values.get("key").unwrap().len(), 1);

        values.add_value("key", None);
        assert_eq!(values.get("key").unwrap().len(), 2);
    }

    #[test]
    fn values_del() {
        let mut values = Values::new();
        values.set_value("key", Some("value".to_string()));
        values.add_value("key", None);
        values.del_value("key");
        assert_eq!(values.len(), 0);

        let mut values2 = Values::new();
        values2.set_value("key", Some("value".to_string()));
        values2.add_value("key", None);
        values2.set_value("key2", Some("value".to_string()));
        values2.add_value("key2", None);

        values2.del_value("key");
        assert_eq!(values2.len(), 1);
    }

    #[test]
    fn values_get() {
        let mut values = Values::new();
        values.set_value("key", Some("value".to_string()));
        values.add_value("key", None);
        assert_eq!(values.get_value("key"), Some("value".to_string()));
    }
}
