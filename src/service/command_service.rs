use crate::*;

impl CommandService for Hget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get(&self.table, &self.key) {
            Ok(Some(value)) => value.into(),
            Ok(None) => KvError::NotFound(self.table, self.key).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match self.pair {
            Some(pair) => match store.set(&self.table, pair.key, pair.value.unwrap_or_default()) {
                Ok(Some(value)) => value.into(),
                Ok(None) => Value::default().into(),
                Err(e) => e.into(),
            },
            None => Value::default().into(),
        }
    }
}

impl CommandService for Hgetall {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.get_all(&self.table) {
            Ok(pairs) => pairs.into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmget {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .into_iter()
            .map(|key| match store.get(&self.table, &key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hmset {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.pairs
            .into_iter()
            .map(
                |pair| match store.set(&self.table, pair.key, pair.value.unwrap_or_default()) {
                    Ok(Some(v)) => v,
                    _ => Value::default(),
                },
            )
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.del(&self.table, &self.key) {
            Ok(Some(value)) => value.into(),
            _ => Value::default().into(),
        }
    }
}

impl CommandService for Hmdel {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .into_iter()
            .map(|key| match store.del(&self.table, &key) {
                Ok(Some(v)) => v,
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

impl CommandService for Hexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        match store.contains(&self.table, &self.key) {
            Ok(v) => Value::from(v).into(),
            Err(e) => e.into(),
        }
    }
}

impl CommandService for Hmexist {
    fn execute(self, store: &impl Storage) -> CommandResponse {
        self.keys
            .into_iter()
            .map(|key| match store.contains(&self.table, &key) {
                Ok(v) => v.into(),
                _ => Value::default(),
            })
            .collect::<Vec<_>>()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_request::RequestData;

    #[test]
    fn hset_should_work() {
        let store = MemTable::new();
        let request = CommandRequest::new_hset("t1", "hello", "world".into());
        let response = dispatch(request.clone(), &store);
        assert_response_ok(response, &[Value::default()], &[]);

        let response = dispatch(request, &store);
        assert_response_ok(response, &["world".into()], &[]);
    }

    #[test]
    fn hget_should_work() {
        let store = MemTable::new();
        let request = CommandRequest::new_hset("score", "math", 10.into());
        let response = dispatch(request, &store);
        assert_response_ok(response, &[Value::default()], &[]);

        let request = CommandRequest::new_hget("score", "math");
        let response = dispatch(request, &store);
        assert_response_ok(response, &[10.into()], &[]);
    }

    #[test]
    fn hget_with_non_existing_key_should_return_404() {
        let store = MemTable::new();
        let request = CommandRequest::new_hget("score", "math");
        let response = dispatch(request, &store);
        assert_response_error(response, 404, "Not found");
    }

    #[test]
    fn hget_all_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "math", 10.into()),
            CommandRequest::new_hset("score", "english", 20.into()),
            CommandRequest::new_hset("score", "chinese", 30.into()),
            CommandRequest::new_hset("score", "math", 40.into()),
        ];

        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let request = CommandRequest::new_hget_all("score");
        let response = dispatch(request, &store);

        let pairs = vec![
            KvPair::new("chinese", 30.into()),
            KvPair::new("english", 20.into()),
            KvPair::new("math", 40.into()),
        ];
        assert_response_ok(response, &[], &pairs);
    }

    #[test]
    fn hmset_should_work() {
        let store = MemTable::new();
        let pairs = vec![
            KvPair::new("math", 10.into()),
            KvPair::new("english", 20.into()),
            KvPair::new("chinese", 30.into()),
            KvPair::new("math", 40.into()),
        ];
        let request = CommandRequest::new_hmset("score", pairs);
        let response = dispatch(request, &store);

        let values = vec![Value::default(), Value::default(), Value::default(), 10.into()];
        assert_response_ok(response, &values, &[]);
    }

    #[test]
    fn hmget_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "math", 10.into()),
            CommandRequest::new_hset("score", "english", 20.into()),
            CommandRequest::new_hset("score", "chinese", 30.into()),
            CommandRequest::new_hset("score", "math", 40.into()),
        ];

        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let request = CommandRequest::new_hmget("score", vec!["math".into(), "chinese".into()]);
        let response = dispatch(request, &store);

        let values = vec![40.into(), 30.into()];
        assert_response_ok(response, &values, &[]);
    }

    #[test]
    fn hdel_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "math", 40.into());

        dispatch(cmd, &store);

        let request = CommandRequest::new_hdel("score", "math");
        let response = dispatch(request, &store);
        assert_response_ok(response, &[40.into()], &[]);

        let request = CommandRequest::new_hget("score", "math");
        let response = dispatch(request, &store);
        assert_response_error(response, 404, "Not found");

        let request = CommandRequest::new_hdel("score", "math");
        let response = dispatch(request, &store);
        assert_response_ok(response, &[Value::default()], &[]);
    }

    #[test]
    fn hmdel_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "math", 10.into()),
            CommandRequest::new_hset("score", "english", 20.into()),
            CommandRequest::new_hset("score", "chinese", 30.into()),
            CommandRequest::new_hset("score", "math", 40.into()),
        ];

        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let request = CommandRequest::new_hmdel("score", vec!["math".into(), "chinese".into()]);
        let response = dispatch(request, &store);

        let values = vec![40.into(), 30.into()];
        assert_response_ok(response, &values, &[]);

        let request = CommandRequest::new_hget_all("score");
        let response = dispatch(request, &store);

        let pairs = vec![KvPair::new("english", 20.into())];
        assert_response_ok(response, &[], &pairs);
    }

    #[test]
    fn hexist_should_work() {
        let store = MemTable::new();
        let cmd = CommandRequest::new_hset("score", "math", 40.into());

        dispatch(cmd, &store);

        let request = CommandRequest::new_hexist("score", "math");
        let response = dispatch(request, &store);
        assert_response_ok(response, &[true.into()], &[]);

        let request = CommandRequest::new_hexist("score", "english");
        let response = dispatch(request, &store);
        assert_response_ok(response, &[false.into()], &[]);
    }

    #[test]
    fn hmexist_should_work() {
        let store = MemTable::new();
        let cmds = vec![
            CommandRequest::new_hset("score", "math", 10.into()),
            CommandRequest::new_hset("score", "english", 20.into()),
            CommandRequest::new_hset("score", "chinese", 30.into()),
        ];

        for cmd in cmds {
            dispatch(cmd, &store);
        }

        let request = CommandRequest::new_hmexist("score", vec!["math".into(), "art".into(), "chinese".into()]);
        let response = dispatch(request, &store);

        let values = vec![true.into(), false.into(), true.into()];
        assert_response_ok(response, &values, &[]);
    }
}
