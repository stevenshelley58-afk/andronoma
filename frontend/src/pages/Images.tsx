const gallery = [
  {
    title: "Futuristic workspace",
    description: "Hero image exploring collaboration between humans and AI systems.",
  },
  {
    title: "Data activation",
    description: "Concept art depicting data flowing into actionable insights.",
  },
];

export default function Images() {
  return (
    <div className="card">
      <h2>Image Generation</h2>
      <p>Curated visual options ready for QA review.</p>
      <div className="stage-grid">
        {gallery.map((image) => (
          <div key={image.title} className="stage-card">
            <h3>{image.title}</h3>
            <p>{image.description}</p>
            <span className="badge">Variation</span>
          </div>
        ))}
      </div>
    </div>
  );
}
