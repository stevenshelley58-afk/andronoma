const checks = [
  {
    name: "Brand safety",
    result: "passed",
    notes: "Copy references approved messaging pillars only.",
  },
  {
    name: "Budget adherence",
    result: "passed",
    notes: "Projected spend within configured thresholds.",
  },
  {
    name: "Accessibility",
    result: "attention",
    notes: "Image alt text requires manual review.",
  },
];

export default function QA() {
  return (
    <div className="card">
      <h2>Quality Automation</h2>
      <p>Every run is gated by the QA checklist before export.</p>
      <table>
        <thead>
          <tr>
            <th>Check</th>
            <th>Result</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          {checks.map((check) => (
            <tr key={check.name}>
              <td>{check.name}</td>
              <td>{check.result}</td>
              <td>{check.notes}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
