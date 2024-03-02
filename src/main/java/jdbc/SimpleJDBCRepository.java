package jdbc;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class SimpleJDBCRepository {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private Statement st = null;

    private static String create = "INSERT INTO myusers (firstname, lastname, age) VALUES (?, ?, ?)";
    private static String update = "UPDATE myusers SET firstname = ?, lastname = ?, age = ? WHERE id = ?";
    private static String delete = "DELETE FROM myusers WHERE id = ?";
    private static String findById = "SELECT * FROM myusers WHERE id = ?";
    private static String findByName = "SELECT * FROM myusers WHERE firstname = ?";
    private static String findAll = "SELECT * FROM myusers";

    public Long createUser(User user) {
        Long result = null;
        try (Connection connection = CustomDataSource.getInstance().getConnection();
             PreparedStatement ps = connection.prepareStatement(create, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, user.getFirstName());
            ps.setString(2, user.getLastName());
            ps.setInt(3, user.getAge());
            ps.executeUpdate();
            ResultSet rs = ps.getGeneratedKeys();
            if (rs.next()) {
                result = rs.getLong(1);
            }
        } catch (SQLException throwables) {
            throwRuntimeException(throwables);
        }
        return result;
    }

    public User findUserById(Long userId) {
        User user = null;
        ResultSet rs;
        try (Connection connection = CustomDataSource.getInstance().getConnection();
             PreparedStatement ps = connection.prepareStatement(findById)) {
            ps.setLong(1, userId);
            rs = ps.executeQuery();
            if (!rs.next())
                throw new SQLException("No such users");

            String firstname = rs.getString("firstname");
            String lastname = rs.getString("lastname");
            int age = rs.getInt("age");

            user = new User(userId, firstname, lastname, age);
        } catch (SQLException throwables) {
            throwRuntimeException(throwables);
        }
        return user;
    }

    public User findUserByName(String userName) {
        ResultSet rs = null;
        User user = null;
        try (Connection connection = CustomDataSource.getInstance().getConnection();
             PreparedStatement ps = connection.prepareStatement(findByName)) {
            ps.setString(1, userName);
            rs = ps.executeQuery();
            if (!rs.next())
                throw new SQLException("No such users");

            String firstname = rs.getString("firstname");
            String lastname = rs.getString("lastname");
            int age = rs.getInt("age");
            Long id = Long.parseLong(rs.getString("id"));

            user = new User(id, firstname, lastname, age);
        } catch (SQLException throwables) {
            throwRuntimeException(throwables);
        }
        return user;
    }

    public List<User> findAllUser() {
        List<User> users = null;
        ResultSet rs = null;
        try (Connection connection = CustomDataSource.getInstance().getConnection();
             Statement st = connection.createStatement()) {
            rs = st.executeQuery(findAll);
            users = new ArrayList<>();
            while (rs.next()) {
                Long id = rs.getLong("id");
                String firstName = rs.getString("firstname");
                String lastName = rs.getString("lastname");
                int age = rs.getInt("age");
                users.add(new User(id, firstName, lastName, age));
            }
        } catch (SQLException throwables) {
            throwRuntimeException(throwables);
        }
        return users;
    }

    public User updateUser(User user) {
        try (Connection connection = CustomDataSource.getInstance().getConnection();
             PreparedStatement ps = connection.prepareStatement(update)) {
            ps.setString(1, user.getFirstName());
            ps.setString(2, user.getLastName());
            ps.setInt(3, user.getAge());
            ps.setLong(4, user.getId());
            if (ps.executeUpdate() == 0)
                throw new SQLException("No such user exists");
        } catch (SQLException throwables) {
            throwRuntimeException(throwables);
        }
        return user;
    }

    public void deleteUser(Long userId) {
        try (Connection connection = CustomDataSource.getInstance().getConnection();
             PreparedStatement ps = connection.prepareStatement(delete)) {
            ps.setLong(1, userId);
            if (ps.executeUpdate() == 0)
                throw new SQLException("No such user exists");
        } catch (SQLException throwables) {
            throwRuntimeException(throwables);
        }
    }

    private void throwRuntimeException(Exception e) {
        String message = String.format("%s: %s", e.getClass().getName(), e.getMessage());
        if (e.getCause() != null) {
            message += String.format("\nCause: %s: %s", e.getCause().getClass().getName(), e.getCause().getMessage());
        }
        throw new RuntimeException(message);
    }
}