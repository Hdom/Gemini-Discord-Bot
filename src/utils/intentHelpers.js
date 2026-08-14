/**
 * Helper to detect if a message content expresses an intent to generate or create an image.
 * @param {string} content - The message content.
 * @returns {boolean} True if the message expresses image generation intent.
 */
export function expressesImageGenerationIntent(content) {
  if (!content) return false;
  const text = content.toLowerCase();

  // Common phrases for generating/creating/drawing/painting/rendering images
  const patterns = [
    /\b(generate|create|draw|paint|make|render|depict)\b.*\b(image|picture|photo|illustration|drawing|painting|artwork|portrait|sketch|scene|wallpaper|graphic|canvas|art)\b/,
    /\b(draw|paint|render)\s+(a|an|the|some)\b/,
    /\b(generate|create)\s+(a|an|the|some)\s+(image|picture|photo|illustration|drawing|painting|artwork|portrait|sketch|scene|wallpaper|graphic|canvas|art)\b/,
    /\b(show me a picture of|show me an image of|show me a drawing of)\b/
  ];

  return patterns.some((pattern) => pattern.test(text));
}
