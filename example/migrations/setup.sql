CREATE TYPE project_tier AS ENUM (
    'free',
    'premium',
    'ultimate'
);

CREATE TABLE IF NOT EXISTS "projects" (
    "id" UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    "created_at" TIMESTAMP DEFAULT NOW(),
    "name" VARCHAR(255) NOT NULL,
    "description" VARCHAR(500),
    "tier" project_tier NOT NULL DEFAULT 'free'
);

INSERT INTO projects (
    name,
    description,
    tier
) VALUES (
    'Project 1',
    'Description for project 1',
    'free'
);

INSERT INTO projects (
    name,
    description,
    tier
) VALUES (
    'Project 2',
    'Description for project 2',
    'premium'
);

INSERT INTO projects (
    name,
    description,
    tier
) VALUES (
    'Project 3',
    'Description for project 3',
    'free'
);

INSERT INTO projects (
    name,
    description,
    tier
) VALUES (
    'Project 4',
    'Description for project 4',
    'premium'
);

INSERT INTO projects (
    name,
    description,
    tier
) VALUES (
    'Project 5',
    NULL,
    'ultimate'
);
