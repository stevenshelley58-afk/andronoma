const exports = [
  {
    name: "Creative package",
    status: "ready",
    link: "s3://andronoma/campaign.zip",
  },
  {
    name: "Audience CSV",
    status: "ready",
    link: "s3://andronoma/audiences.csv",
  },
];

export default function ExportPage() {
  return (
    <div className="card">
      <h2>Export Center</h2>
      <p>Download campaign artifacts once QA approves the run.</p>
      <table>
        <thead>
          <tr>
            <th>Asset</th>
            <th>Status</th>
            <th>Location</th>
          </tr>
        </thead>
        <tbody>
          {exports.map((item) => (
            <tr key={item.name}>
              <td>{item.name}</td>
              <td>{item.status}</td>
              <td>{item.link}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
