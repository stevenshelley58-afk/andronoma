const sampleSegments = [
  {
    name: "CIO Innovators",
    description: "Enterprise technology leaders exploring AI-driven marketing automation.",
    size: "15k",
  },
  {
    name: "Growth Marketers",
    description: "Performance marketers testing multi-channel creative variations.",
    size: "32k",
  },
];

export default function Audiences() {
  return (
    <div className="card">
      <h2>Audience Intelligence</h2>
      <p>Review the personas and segments generated during the NLP stages.</p>
      <div className="stage-grid">
        {sampleSegments.map((segment) => (
          <div key={segment.name} className="stage-card">
            <h3>{segment.name}</h3>
            <p>{segment.description}</p>
            <p>Reach: {segment.size}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
