export type StringRecord = Record<string, string>;

function coerceString(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number" || typeof value === "bigint") {
    return String(value);
  }
  if (typeof value === "boolean") {
    return value ? "true" : "false";
  }
  return JSON.stringify(value);
}

export function normalizeRecordArray(value: unknown): StringRecord[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const result: StringRecord[] = [];
  for (const item of value) {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
      continue;
    }
    const entries = Object.entries(item as Record<string, unknown>);
    if (!entries.length) {
      continue;
    }
    const record: StringRecord = {};
    for (const [key, raw] of entries) {
      record[key] = coerceString(raw);
    }
    result.push(record);
  }
  return result;
}

function splitCsvLine(line: string): string[] {
  const values: string[] = [];
  let current = "";
  let inQuotes = false;
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index];
    if (char === "\"") {
      if (inQuotes && line[index + 1] === "\"") {
        current += "\"";
        index += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      values.push(current);
      current = "";
    } else {
      current += char;
    }
  }
  values.push(current);
  return values.map((value) => value.replace(/\r/g, "").trim());
}

export function parseCsvRecords(csv: string): StringRecord[] {
  const text = csv.trim();
  if (!text || !text.includes(",")) {
    return [];
  }
  const rows: StringRecord[] = [];
  const normalized = text.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n").filter((line) => line.length > 0);
  if (lines.length < 2) {
    return [];
  }
  const headers = splitCsvLine(lines[0]).map((header, index) => header || `column_${index + 1}`);
  for (let i = 1; i < lines.length; i += 1) {
    const cells = splitCsvLine(lines[i]);
    if (cells.length === 0) {
      continue;
    }
    const record: StringRecord = {};
    headers.forEach((header, index) => {
      record[header] = coerceString(cells[index] ?? "");
    });
    rows.push(record);
  }
  return rows;
}

function parseCsvIfLikely(value: unknown): StringRecord[] {
  if (typeof value !== "string") {
    return [];
  }
  if (!value.includes("\n") || !value.includes(",")) {
    return [];
  }
  return parseCsvRecords(value);
}

export function findRecordsByColumnKeywords(root: unknown, keywords: string[]): StringRecord[] {
  if (!root) {
    return [];
  }
  const required = keywords.map((keyword) => keyword.toLowerCase());
  const visited = new Set<unknown>();

  const matchesKeywords = (record: StringRecord | undefined) => {
    if (!record) return false;
    const columns = Object.keys(record).map((key) => key.toLowerCase());
    return required.every((keyword) => columns.some((column) => column.includes(keyword)));
  };

  const traverse = (value: unknown): StringRecord[] => {
    if (value === null || value === undefined) {
      return [];
    }
    if (typeof value === "string") {
      const parsed = parseCsvIfLikely(value);
      if (parsed.length && (!required.length || matchesKeywords(parsed[0]))) {
        return parsed;
      }
      return [];
    }
    if (typeof value !== "object") {
      return [];
    }
    if (visited.has(value)) {
      return [];
    }
    visited.add(value);

    if (Array.isArray(value)) {
      const normalized = normalizeRecordArray(value);
      if (normalized.length && (!required.length || matchesKeywords(normalized[0]))) {
        return normalized;
      }
      for (const item of value) {
        const nested = traverse(item);
        if (nested.length) {
          return nested;
        }
      }
      return [];
    }

    const entries = Object.values(value as Record<string, unknown>);
    for (const entry of entries) {
      const nested = traverse(entry);
      if (nested.length) {
        return nested;
      }
    }
    return [];
  };

  return traverse(root);
}

export function uniqueColumns(records: StringRecord[]): string[] {
  const seen = new Set<string>();
  const columns: string[] = [];
  for (const record of records) {
    for (const key of Object.keys(record)) {
      if (!seen.has(key)) {
        seen.add(key);
        columns.push(key);
      }
    }
  }
  return columns;
}
