// Copyright 2024 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use multimap::MultiMap;
use risingwave_expr::table_function::repeat;
use risingwave_stream::executor::{ExecutorInfo, ProjectSetExecutor};

use crate::prelude::*;

const CHUNK_SIZE: usize = 1024;

fn create_executor() -> (MessageSender, BoxedMessageStream) {
    let schema = Schema {
        fields: vec![
            Field::unnamed(DataType::Int64),
            Field::unnamed(DataType::Int64),
        ],
    };
    let (tx, source) = MockSource::channel(schema, PkIndices::new());

    let test_expr = build_from_pretty("(add:int8 $0:int8 $1:int8)").into_inner();
    let test_expr_watermark = build_from_pretty("(add:int8 $0:int8 1:int8)").into_inner();
    let tf1 = repeat(build_from_pretty("1:int4").into_inner(), 1);
    let tf2 = repeat(build_from_pretty("2:int4").into_inner(), 2);

    let info = ExecutorInfo {
        schema: Schema {
            fields: vec![
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int64),
                Field::unnamed(DataType::Int32),
                Field::unnamed(DataType::Int32),
            ],
        },
        pk_indices: vec![],
        identity: "ProjectSetExecutor".to_string(),
    };

    let project_set = Box::new(ProjectSetExecutor::new(
        ActorContext::for_test(123),
        info,
        Box::new(source),
        vec![
            test_expr.into(),
            test_expr_watermark.into(),
            tf1.into(),
            tf2.into(),
        ],
        CHUNK_SIZE,
        MultiMap::from_iter(std::iter::once((0, 1))),
        vec![],
    ));
    (tx, project_set.execute())
}

#[tokio::test]
async fn test_project_set() {
    let (mut tx, mut project_set) = create_executor();

    tx.push_chunk(StreamChunk::from_pretty(
        " I I
        + 1 4
        + 2 5
        + 3 6",
    ));
    tx.push_int64_watermark(0, 3);
    tx.push_chunk(StreamChunk::from_pretty(
        " I I
        + 7 8
        - 3 6",
    ));

    check_until_pending(
        &mut project_set,
        expect_test::expect![[r#"
            - !chunk |-
              +---+---+---+---+---+---+
              | + | 0 | 5 | 2 | 1 | 2 |
              | + | 1 | 5 | 2 |   | 2 |
              | + | 0 | 7 | 3 | 1 | 2 |
              | + | 1 | 7 | 3 |   | 2 |
              | + | 0 | 9 | 4 | 1 | 2 |
              | + | 1 | 9 | 4 |   | 2 |
              +---+---+---+---+---+---+
            - !watermark
              col_idx: 2
              val: '4'
            - !chunk |-
              +---+---+----+---+---+---+
              | + | 0 | 15 | 8 | 1 | 2 |
              | + | 1 | 15 | 8 |   | 2 |
              | - | 0 | 9  | 4 | 1 | 2 |
              | - | 1 | 9  | 4 |   | 2 |
              +---+---+----+---+---+---+
        "#]],
        SnapshotOptions::default(),
    );
}
