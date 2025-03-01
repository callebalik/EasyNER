# EasyNer Server Guidelines

## Server Architecture
- Flask-based GUI for data interaction and inspection
- RESTful API design for data operations
- Use DBDataExchanger for database interactions
- Implement data_model module patterns
- Ensure proper thread safety and connection management

## Frontend Development
- JavaScript Methods:
  - Create reusable components across pages
  - Implement modular design patterns
  - Use async/await for data operations
  - Handle errors gracefully with user feedback
  - Cache appropriate data client-side

## Styling
- Write CSS using SCSS methodology
- Organize styles in partial files
- Import via central styling.scss
- Follow BEM naming conventions
- Maintain responsive design principles

## Security
- Implement proper input validation
- Use parameterized queries
- Handle CORS appropriately
- Validate all API endpoints
- Implement rate limiting where needed

## Performance
- Optimize database queries for GUI operations
- Implement proper connection pooling
- Cache frequently accessed data
- Use pagination for large datasets
- Monitor server resource usage

## Error Handling
- Provide meaningful error messages to users
- Log server-side errors appropriately
- Implement proper status codes
- Handle connection timeouts gracefully
- Maintain audit logs for critical operations