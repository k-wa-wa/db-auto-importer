
-- 1. シンプルな親子関係(child → parent → grandparent)をテストするためのテーブル
CREATE TABLE organizations (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    organization_id INT,
    created_at TIMESTAMP,
    CONSTRAINT fk_organization_id FOREIGN KEY (organization_id) REFERENCES organizations(id)
);
CREATE TABLE posts (
    id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    content TEXT,
    user_id INT,
    published BOOLEAN,
    CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES users(id)
);

-- 2. 多対多の関係をテストするためのテーブル
CREATE TABLE products (
    id INT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    price DECIMAL(10, 2)
);
CREATE TABLE tags (
    id INT PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL
);
CREATE TABLE product_tags (
    product_id INT,
    tag_id INT,
    created_at TIMESTAMP,
    PRIMARY KEY (product_id, tag_id),
    CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES products(id),
    CONSTRAINT fk_tag_id FOREIGN KEY (tag_id) REFERENCES tags(id)
);
