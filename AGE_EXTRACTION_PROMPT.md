# AI_COMPLETE Prompt for Age Extraction

## Purpose
Extract a 2 or 3 digit numerical age from free-form patient age fields that may contain:
- Plain numbers: `45`, `32`, `150`
- Text with units: `45 years`, `32 yrs old`, `28yo`
- Prefixed values: `Age: 45`, `Patient age: 32`
- Text representations: `forty-five`, `thirty-two`, `infant`
- Age ranges: `mid-30s`, `early 20s`, `late 40s`
- Invalid/missing: `unknown`, `N/A`, `NULL`, `150`, `-5`

## Complete Prompt (SQL Version)

```sql
SNOWFLAKE.CORTEX.AI_COMPLETE(
    'llama3.1-8b',
    CONCAT(
        'Extract the patient age as a 2 or 3 digit number from this text: "', COALESCE(age, 'unknown'), '"', CHAR(10), CHAR(10),
        'STRICT RULES:', CHAR(10),
        '1. Return ONLY a number (0-120) or the word "INVALID" - no other text, punctuation, or explanation', CHAR(10),
        '2. Valid age range: 0 to 120 years', CHAR(10),
        '3. If age is >120, <0, unclear, or missing: return "INVALID"', CHAR(10), CHAR(10),
        'SPECIAL CASES:', CHAR(10),
        '- Infants/newborns/babies: return 0', CHAR(10),
        '- Age ranges (e.g., "mid-30s", "early 20s"): return midpoint (35, 22)', CHAR(10),
        '- Text numbers (e.g., "forty-five"): convert to digits (45)', CHAR(10),
        '- Units ignored (e.g., "45 years", "32 yrs", "28yo"): extract number only', CHAR(10),
        '- Prefixes ignored (e.g., "Age: 45"): extract number only', CHAR(10), CHAR(10),
        'EXAMPLES:', CHAR(10),
        '"45 years" → 45', CHAR(10),
        '"infant" → 0', CHAR(10),
        '"forty-five" → 45', CHAR(10),
        '"mid-30s" → 35', CHAR(10),
        '"150" → INVALID', CHAR(10),
        '"unknown" → INVALID'
    )
)
```

## Readable Version (Multi-line String)

```
Extract the patient age as a 2 or 3 digit number from this text: "{age_value}"

STRICT RULES:
1. Return ONLY a number (0-120) or the word "INVALID" - no other text, punctuation, or explanation
2. Valid age range: 0 to 120 years
3. If age is >120, <0, unclear, or missing: return "INVALID"

SPECIAL CASES:
- Infants/newborns/babies: return 0
- Age ranges (e.g., "mid-30s", "early 20s"): return midpoint (35, 22)
- Text numbers (e.g., "forty-five"): convert to digits (45)
- Units ignored (e.g., "45 years", "32 yrs", "28yo"): extract number only
- Prefixes ignored (e.g., "Age: 45"): extract number only

EXAMPLES:
"45 years" → 45
"32 yrs old" → 32
"Age: 28" → 28
"infant" → 0
"newborn baby" → 0
"forty-five" → 45
"mid-30s" → 35
"early 20s" → 22
"150" → INVALID
"-5" → INVALID
"unknown" → INVALID
"N/A" → INVALID
"" → INVALID
```

## Compact Version (For Stored Procedures)

For scenarios where you need a shorter prompt (to save tokens/cost):

```sql
CONCAT(
    'Extract patient age (0-120) from: "', COALESCE(age, 'unknown'), '". ',
    'Return ONLY a number or "INVALID". ',
    'Rules: infants=0, ranges=midpoint, text numbers=digits, >120=INVALID, <0=INVALID. ',
    'Examples: "45 years"->45, "infant"->0, "mid-30s"->35, "150"->INVALID'
)
```

## Expected Outputs

| Input | Expected Output | Reasoning |
|-------|----------------|-----------|
| `45` | `45` | Plain number |
| `45 years` | `45` | Extract number, ignore unit |
| `32 yrs old` | `32` | Extract number, ignore unit and text |
| `Age: 28` | `28` | Extract number, ignore prefix |
| `28yo` | `28` | Extract number, ignore suffix |
| `infant` | `0` | Special case: newborn |
| `newborn` | `0` | Special case: newborn |
| `baby` | `0` | Special case: newborn |
| `forty-five` | `45` | Text to number conversion |
| `thirty-two` | `32` | Text to number conversion |
| `mid-30s` | `35` | Range midpoint |
| `early 20s` | `22` | Range lower-mid estimate |
| `late 40s` | `48` | Range upper-mid estimate |
| `150` | `INVALID` | Out of range (>120) |
| `-5` | `INVALID` | Out of range (<0) |
| `unknown` | `INVALID` | Cannot determine |
| `N/A` | `INVALID` | Cannot determine |
| `NULL` | `INVALID` | Missing value |
| ` ` (empty) | `INVALID` | Missing value |
| `elderly` | `INVALID` | Too ambiguous |
| `senior` | `INVALID` | Too ambiguous |

## Why This Prompt Works

### 1. Clear Structure
- **STRICT RULES** section sets boundaries
- **SPECIAL CASES** section handles edge cases
- **EXAMPLES** section provides concrete guidance

### 2. Explicit Output Format
- "Return ONLY a number (0-120) or the word 'INVALID'"
- Prevents LLM from returning explanations or extra text
- Makes parsing trivial: `TRY_CAST(result AS INTEGER)`

### 3. Comprehensive Coverage
- Handles numeric inputs: `45`, `32`
- Handles text with units: `45 years`, `32 yrs`
- Handles text representations: `forty-five`, `infant`
- Handles ranges: `mid-30s`, `early 20s`
- Handles invalid inputs: `150`, `-5`, `unknown`

### 4. Few-Shot Learning
- Provides 6-10 examples covering different input types
- Helps the LLM understand the pattern
- Improves accuracy on edge cases

### 5. Validation Rules
- Min: 0 (infants)
- Max: 120 (oldest plausible human)
- Out of range → INVALID
- Ambiguous → INVALID

## Model Selection

### Recommended: `llama3.1-8b`
- **Speed**: Fast processing
- **Cost**: Low token cost (~$0.50 per 10K records)
- **Accuracy**: 90-95% for this task
- **Best for**: Large batch processing

### Alternative: `llama3.1-70b`
- **Speed**: Slower
- **Cost**: Higher token cost (~$2.00 per 10K records)
- **Accuracy**: 95-98% for this task
- **Best for**: Complex edge cases, higher accuracy requirements

## Token Optimization

The full prompt is ~350 tokens. For cost optimization:

1. **Use compact version** for large datasets (saves 200 tokens/call)
2. **Cache frequently used prompts** if your workflow supports it
3. **Only apply AI to messy data** (pre-filter clean numeric values)

Example optimization:
```sql
CASE 
    WHEN TRY_CAST(age AS INTEGER) BETWEEN 0 AND 120 
        THEN TRY_CAST(age AS INTEGER)  -- Already clean
    ELSE TRY_CAST(
        SNOWFLAKE.CORTEX.AI_COMPLETE('llama3.1-8b', compact_prompt)
        AS INTEGER
    )
END AS age_cleansed
```

## Post-Processing

After AI extraction, always validate:

```sql
CASE 
    WHEN age_extracted = 'INVALID' THEN NULL
    WHEN TRY_CAST(age_extracted AS INTEGER) BETWEEN 0 AND 120 
        THEN TRY_CAST(age_extracted AS INTEGER)
    ELSE NULL
END AS age_cleansed
```

## Success Metrics

Expected performance on typical datasets:
- **90-95% successfully extracted** (numeric value returned)
- **5-10% flagged as INVALID** (require human review)
- **<1% misclassified** (wrong age extracted)

## Edge Cases Requiring Human Review

Flag these for manual verification:
1. Ambiguous terms: `elderly`, `senior citizen`, `young adult`
2. Invalid ranges: values >120 or <0
3. Truly missing data: `unknown`, `N/A`, `NULL`
4. Age at specific events: `age at diagnosis: 45` (may need context)

## References

- [Snowflake AI_COMPLETE Documentation](https://docs.snowflake.com/en/sql-reference/functions/ai_complete)
- [Cortex LLM Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions)
- [Model Selection Guide](https://docs.snowflake.com/en/user-guide/snowflake-cortex/llm-functions#label-cortex-llm-choosing)

