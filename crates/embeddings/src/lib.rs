use std::path::Path;


use ndarray::Array;
use ort::session::{Session, builder::GraphOptimizationLevel};
use ort::value::Value;
use tokenizers::Tokenizer;

#[derive(Debug, Clone, Copy)]
pub enum PoolingMode {
    Mean,
    Cls,
}

pub struct Embedder {
    tokenizer: Tokenizer,
    session: Session,
    pooling: PoolingMode,
    query_prefix: Option<String>,
}

impl Embedder {
    pub fn new(model_dir: impl AsRef<Path>, pooling: Option<&str>, query_prefix: Option<&str>) -> anyhow::Result<Self> {
        let model_dir = model_dir.as_ref();
        let pooling = match pooling.unwrap_or("mean").to_lowercase().as_str() {
            "cls" => PoolingMode::Cls,
            _ => PoolingMode::Mean,
        };
        let query_prefix = query_prefix.map(|s| s.to_string());

        let tokenizer_path = model_dir.join("tokenizer.json");
        let model_path = model_dir.join("model.onnx");

        if !tokenizer_path.exists() {
            anyhow::bail!(
                "tokenizer.json not found in {}",
                model_dir.display()
            );
        }
        if !model_path.exists() {
            anyhow::bail!(
                "model.onnx not found in {}",
                model_dir.display()
            );
        }

        let tokenizer = Tokenizer::from_file(&tokenizer_path)
            .map_err(|e| anyhow::anyhow!("failed to load tokenizer: {}", e))?;

        let session = Session::builder()?
            .with_optimization_level(GraphOptimizationLevel::Level3)?
            .with_intra_threads(4)?
            .commit_from_file(model_path)?;

        Ok(Self { tokenizer, session, pooling, query_prefix })
    }

    pub fn embed(&mut self, text: &str, is_query: bool) -> anyhow::Result<Vec<f32>> {
        let text_cow = if is_query {
             if let Some(prefix) = &self.query_prefix {
                 std::borrow::Cow::Owned(format!("{}{}", prefix, text))
             } else {
                 std::borrow::Cow::Borrowed(text)
             }
        } else {
            std::borrow::Cow::Borrowed(text)
        };

        let encoding = self
            .tokenizer
            .encode(text_cow.as_ref(), true)
            .map_err(|e| anyhow::anyhow!("encoding error: {}", e))?;

        let input_ids: Vec<i64> = encoding
            .get_ids()
            .iter()
            .map(|&x| x as i64)
            .collect();
        let attention_mask: Vec<i64> = encoding
            .get_attention_mask()
            .iter()
            .map(|&x| x as i64)
            .collect();
        let token_type_ids: Vec<i64> = encoding
            .get_type_ids()
            .iter()
            .map(|&x| x as i64)
            .collect();

        let batch_size = 1;
        let seq_len = input_ids.len();

        let input_ids_array =
            Array::from_shape_vec((batch_size, seq_len), input_ids)?;
        let attention_mask_array =
            Array::from_shape_vec((batch_size, seq_len), attention_mask)?;
        let token_type_ids_array =
            Array::from_shape_vec((batch_size, seq_len), token_type_ids)?;

        let input_ids_val = Value::from_array(input_ids_array)?;
        let attention_mask_val = Value::from_array(attention_mask_array)?;
        let token_type_ids_val = Value::from_array(token_type_ids_array)?;

        let has_token_type_ids = self.session.inputs().iter().any(|i| i.name() == "token_type_ids");

        let outputs = if has_token_type_ids {
            self.session.run(ort::inputs![
                "input_ids" => input_ids_val,
                "attention_mask" => attention_mask_val,
                "token_type_ids" => token_type_ids_val,
            ])?
        } else {
            self.session.run(ort::inputs![
                "input_ids" => input_ids_val,
                "attention_mask" => attention_mask_val,
            ])?
        };

        // Extract last_hidden_state (batch, seq_len, hidden_size)
        // Usually output 0 is last_hidden_state
        let (shape, data) = outputs[0]
            .try_extract_tensor::<f32>()?;
        
        let batch = shape[0] as usize;
        let seq = shape[1] as usize;
        let hidden = shape[2] as usize;
        
        assert_eq!(batch, 1);
        
        let mut pooled = vec![0.0f32; hidden];

        match self.pooling {
            PoolingMode::Mean => {
                let mut count = 0.0f32;
                for i in 0..seq {
                    // Check attention mask
                     if encoding.get_attention_mask()[i] == 1 {
                         for j in 0..hidden {
                             pooled[j] += data[i * hidden + j];
                         }
                         count += 1.0;
                     }
                }
                if count > 0.0 {
                    for val in &mut pooled {
                        *val /= count;
                    }
                }
            }
            PoolingMode::Cls => {
                // CLS is at index 0
                for j in 0..hidden {
                    pooled[j] = data[j];
                }
            }
        }

        // Normalize
        let norm: f32 = pooled.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 1e-6 {
            for val in &mut pooled {
                *val /= norm;
            }
        }

        Ok(pooled)
    }
}
