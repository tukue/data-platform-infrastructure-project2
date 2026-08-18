const fs = require('fs');
const path = require('path');

const catalogPath = path.join(__dirname, '..', 'docs', 'event-catalog.json');
const catalog = JSON.parse(fs.readFileSync(catalogPath, 'utf8'));

const errors = [];
const requiredFields = [
  'id',
  'domain',
  'description',
  'producer.component',
  'topic.default',
  'topic.configuration.cdkContext',
  'topic.configuration.environmentVariable',
  'consumers.0.component',
  'schema.format',
  'schema.status',
  'owner.status',
];

function valueAt(object, field) {
  return field.split('.').reduce((value, key) => value?.[key], object);
}

if (!Array.isArray(catalog.events) || catalog.events.length === 0) {
  errors.push('events must be a non-empty array.');
}

const eventIds = new Set();
const topics = new Set();

for (const event of catalog.events ?? []) {
  for (const field of requiredFields) {
    const value = valueAt(event, field);
    if (typeof value !== 'string' || value.trim().length === 0) {
      errors.push(`${event.id ?? '<unknown>'}: ${field} must be a non-empty string.`);
    }
  }

  if (eventIds.has(event.id)) {
    errors.push(`${event.id}: duplicate event id.`);
  }
  eventIds.add(event.id);

  if (topics.has(event.topic?.default)) {
    errors.push(`${event.id}: duplicate default topic ${event.topic.default}.`);
  }
  topics.add(event.topic?.default);

  if (!['defined', 'not-defined'].includes(event.schema?.status)) {
    errors.push(`${event.id}: schema.status must be defined or not-defined.`);
  }

  if (event.schema?.status === 'defined') {
    if (typeof event.schema.location !== 'string' || event.schema.location.length === 0) {
      errors.push(`${event.id}: defined schemas require schema.location.`);
    } else if (!fs.existsSync(path.join(__dirname, '..', event.schema.location))) {
      errors.push(`${event.id}: schema.location does not exist: ${event.schema.location}.`);
    }
  }

  if (!['assigned', 'unassigned'].includes(event.owner?.status)) {
    errors.push(`${event.id}: owner.status must be assigned or unassigned.`);
  }
  if (event.owner?.status === 'assigned' && (typeof event.owner.team !== 'string' || event.owner.team.length === 0)) {
    errors.push(`${event.id}: assigned owners require owner.team.`);
  }
}

if (errors.length > 0) {
  console.error(`Event catalog validation failed:\n- ${errors.join('\n- ')}`);
  process.exit(1);
}

console.log(`Event catalog validation passed for ${catalog.events.length} events.`);
