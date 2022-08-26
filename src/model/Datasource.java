package model;

import org.cef.misc.CefPdfPrintSettings;

import javax.xml.transform.Result;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Datasource {

    private static final String DB_NAME = "music.db";
    private static final String CONNECTION_STRING = "jdbc:sqlite:D:\\MRc\\Java\\music\\" + DB_NAME;
    private static final String TABLE_ALBUMS = "albums";
    private static final String COLUMN_ALBUM_ID = "_id";
    private static final String COLUMN_ALBUM_NAME = "name";
    private static final String COLUMN_ALBUM_ARTIST = "artist";

    private static final String TABLE_ARTISTS = "artists";
    private static final String COLUMN_ARTIST_ID = "_id";
    private static final String COLUMN_ARTIST_NAME = "name";

    public static final String TABLE_SONGS = "songs";
    private static final String COLUMN_SONG_TRACK = "track";
    private static final String COLUMN_SONG_TITLE = "title";
    private static final String COLUMN_SONG_ALBUM = "album";


    public static final int ORDER_BY_ASC = 3;
    public static final int ORDER_BY_DESC = 2;
    public static final int ORDER_BY_NONE = 1;

    /*
    query joins the albums and artists table based on the artist id, which is both tables (col. artist in albums table
    is the id of the artist in the artists table), and selects all records, which are equal to the argument
     */
    public static final String QUERY_ALBUMS_BY_ARTIST_START =
            "SELECT " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS +
                    " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
                    " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID + " WHERE " +
                    TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + " = \"";

    /*
    query for ordering
     */
    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";

    public static final String QUERY_VIEW_SONG_INFO = "SELECT " + COLUMN_ARTIST_NAME + ", " +
            COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + COLUMN_SONG_TITLE + " = \"";

    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + "." + COLUMN_SONG_TITLE +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS +
            "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
            " ORDER BY " +
            TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK;


    /*
  FIRST inner join joins songs and albums where record in table SONGS has = album id to album id from
  table ALBUMS
  SECOND inner join joins table ARTISTS and ALBUMS where col.artist in ALBUMS is = to ID from ARTISTS table.

  query returns artists,their albums and the searched song from the two joins
   */
    public static final String QUERY_ARTIST_OR_SONG_START =
            "SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
                    TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", " +
                    TABLE_SONGS + "." + COLUMN_SONG_TRACK +
                    " FROM " +
                    TABLE_SONGS + " INNER JOIN " + TABLE_ALBUMS + " ON " +
                    TABLE_SONGS + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
                    " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " +
                    TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +

                    " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_ARTIST_FOR_SONG_SORT =
            " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", "
                    + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";


    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME + ", " +
            COLUMN_SONG_ALBUM + ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + COLUMN_SONG_TITLE + " = ?";

    //--------------
    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS +
            '(' + COLUMN_ARTIST_NAME + ") VALUES(?)";
    public static final String INSERT_ALBUMS = "INSERT INTO " + TABLE_ALBUMS +
            '(' + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") VALUES(?, ?)";

    public static final String INSERT_SONGS = "INSERT INTO " + TABLE_SONGS +
            '(' + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM +
            ") VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
            TABLE_ARTISTS + " WHERE " + COLUMN_ARTIST_NAME + " = ?";

    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " +
            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " = ?";

    public static final String QUERY_ALBUMS_BY_ARTIST_ID = "SELECT * FROM " + TABLE_ALBUMS +
            " WHERE " + COLUMN_ALBUM_ARTIST + " = ? ORDER BY " + COLUMN_ALBUM_NAME + " COLLATE NOCASE";


    private Connection conn;
    private PreparedStatement querySongInfoView;

    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;

    private  PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;

    public boolean open() {
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfoView = conn.prepareStatement(QUERY_VIEW_SONG_INFO_PREP);
            insertIntoArtists = conn.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = conn.prepareStatement(INSERT_ALBUMS, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = conn.prepareStatement(INSERT_SONGS, Statement.RETURN_GENERATED_KEYS);
            queryArtist = conn.prepareStatement(QUERY_ARTIST);
            queryAlbum = conn.prepareStatement(QUERY_ALBUM);
            return true;
        } catch (SQLException e) {
            System.out.println("Couldnt connect to database" + e.getMessage());
            return false;
        }

    }

    public void close() {
        try {
            if (querySongInfoView != null){
                querySongInfoView.close();
            }

            if (insertIntoArtists != null)
                insertIntoArtists.close();

            if(insertIntoAlbums != null)
                insertIntoAlbums.close();

            if (insertIntoSongs != null)
                insertIntoSongs.close();

            if (queryAlbum != null)
                queryAlbum.close();

            if (queryArtist != null)
                queryArtist.close();

            if (conn != null)
                conn.close();


        } catch (SQLException e) {
            System.out.println("Couldnt close the connection " + e.getMessage());
        }
    }

    // function returns all artists from the db
    public List<Artist> queryArtists(int orderBy) {

        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(TABLE_ARTISTS);
        if (orderBy != ORDER_BY_NONE) {
            sb.append(" ORDER BY ");
            sb.append(COLUMN_ARTIST_NAME);
            sb.append(" COLLATE NOCASE ");
            if (orderBy == ORDER_BY_DESC)
                sb.append("DESC");

            else sb.append("ASC");
        }

        /*
        statement and resultset in try method will be automatically closed
        wether there is an exception or not
         */
        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {
            List<Artist> artists = new ArrayList<>();

            while (results.next()) {
                Artist artist = new Artist();
                artist.setId(results.getInt(COLUMN_ARTIST_ID));
                artist.setName(results.getString(COLUMN_ARTIST_NAME));
                artists.add(artist);
            }

            return artists;
        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }


    }

    public List<String> queryAlbumsOfArtist(String artist, int sortOrder) {

        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append(artist);
        sb.append("\"");

        if (sortOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_DESC)
                sb.append("DESC");
            else sb.append("ASC");
        }

        System.out.println("SQL statement = " + sb.toString());
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sb.toString())) {

            List<String> albums = new ArrayList<>();
            while (rs.next()) {
                albums.add(rs.getString(1));
            }
            return albums;

        } catch (SQLException e) {
            System.out.println("Query failed : " + e.getMessage());
            return null;
        }

    }


    // function returns the artist, album and the tracknumber for the required song
    public List<SongArtist> queryArtistsforSong(String songname, int sorOrder) {

        StringBuilder sb = new StringBuilder(QUERY_ARTIST_OR_SONG_START);
        sb.append(songname);
        sb.append("\"");

        if (sorOrder != ORDER_BY_NONE) {
            sb.append(QUERY_ARTIST_FOR_SONG_SORT);
            if (sorOrder == ORDER_BY_DESC)
                sb.append("DESC");
            else sb.append("ASC");

        }
        System.out.println("SQL Statement: " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())) {

            List<SongArtist> songArtists = new ArrayList<>();

            while (results.next()) {
                SongArtist songArtist = new SongArtist();
                songArtist.setArtistName(results.getString(1));
                songArtist.setAlbumname(results.getString(2));
                songArtist.setTrack(results.getInt(3));
                songArtists.add(songArtist);
            }
            return songArtists;

        } catch (SQLException e) {
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }

    }

    public int getCount(String table) {
        String sql = "SELECT COUNT(*) AS COUNT FROM " + table;
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {

            int count = rs.getInt("count");
            System.out.format("Count = %d\n", count);
            return count;

        } catch (SQLException e) {
            System.out.println("Query failed> " + e.getMessage());
            return -1;
        }

    }


    public void querySongMetadata() {
        String sql = "SELECT * FROM " + TABLE_SONGS;

        try (Statement stm = conn.createStatement();
             ResultSet rs = stm.executeQuery(sql)) {
            /* resultsetmetadata returnes information such as column name,
             type, count */
            ResultSetMetaData meta = rs.getMetaData();
            int columCount = meta.getColumnCount();
            for (int i = 1; i <= columCount; i++) {
                System.out.format("Column %d in the songs table is names %s\n",
                        i, meta.getColumnName(i));
            }

        } catch (SQLException e) {
            System.out.println("Query failed> " + e.getMessage());
        }

    }

    /* method creates a view containing song title, artist, album and track num., this
    we dont need to run a join as the result will be saved in a view
    */
    public boolean createViewForSongArtists() {

        try (Statement statement = conn.createStatement()) {

            statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
            return true;

        } catch (SQLException e) {
            System.out.println("Create View failed: " + e.getMessage());
            return false;
        }
    }

    public List<SongArtist> querySongInfoView(String title) {

        try {
            // the 1 in setstring indicates the first occurance of ? mark in the query
            querySongInfoView.setString(1, title);
            ResultSet rs = querySongInfoView.executeQuery();

                List<SongArtist> songArtists = new ArrayList<>();
                while (rs.next()) {
                    SongArtist songArtist = new SongArtist();
                    songArtist.setArtistName(rs.getString(1));
                    songArtist.setAlbumname(rs.getString(2));
                    songArtist.setTrack(rs.getInt(3));
                    songArtists.add(songArtist);
                }

                return songArtists;
            }
             catch (SQLException e) {
            System.out.println("Query failed > " + e.getMessage());
            return null;
        }

    }

    private int insertArtist(String name) throws SQLException {
        queryArtist.setString(1,name);

        //first we check wether the artist exists
        ResultSet results = queryArtist.executeQuery();
        if (results.next())
            //if it already exists we return the id from the 1. column
            return results.getInt(1);
        else {
            //if it is not in table, we insert the artist
            insertIntoArtists.setString(1, name);
            // executeUpdate return the number of rows updated
            int affectedRows = insertIntoArtists.executeUpdate();
            //in this case number of affected rows should be 1 as we insert only 1 artist
            if (affectedRows != 1)
                throw new SQLException("Cant insert artist");

            // we return the generated key (id of newly inserted artist)
            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if (generatedKeys.next())
                return generatedKeys.getInt(1);
            else
                throw new SQLException("Cant get id for artist");


        }
    }


    private int insertAlbum(String name, int artistID) throws SQLException {
        queryAlbum.setString(1,name);

        //first we check wether the album exists
        ResultSet results = queryAlbum.executeQuery();
        if (results.next())
            //if it already exists we return the id from the 1. column
            return results.getInt(1);
        else {
            //if it is not in table, we insert the artist
            insertIntoAlbums.setString(1, name);
            insertIntoAlbums.setInt(2, artistID);
            // executeUpdate return the number of rows updated
            int affectedRows = insertIntoAlbums.executeUpdate();
            //in this case number of affected rows should be 1 as we insert only 1 artist
            if (affectedRows != 1)
                throw new SQLException("Cant insert album");

            // we return the generated key (id of newly inserted artist)
            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if (generatedKeys.next())
                return generatedKeys.getInt(1);
            else
                throw new SQLException("Cant get id for album ");


        }
    }


    private void insertSong(String title, String artist, String album, int track)  {
        try {
            conn.setAutoCommit(false);

            int artistID = insertArtist(artist);
            int albumID = insertAlbum(album,artistID);
            insertIntoSongs.setInt(1, track);
            insertIntoSongs.setString(2,title);
            insertIntoSongs.setInt(3, albumID);
            int affectedRows = insertIntoSongs.executeUpdate();
                //in this case number of affected rows should be 1 as we insert only 1 artist
                if (affectedRows == 1)
                    conn.commit();
                else
                    throw new SQLException("The son insert failed");

        }
        catch (SQLException e){
            System.out.println("Insert song exception: "+ e.getMessage());
            try {
                System.out.println("Preforming rollback");
                conn.rollback();
            } catch (SQLException e2){
                System.out.println("Realy bad" + e.getMessage());
            }
        }
        finally {
            try {
                System.out.println("Resetting default commit behaviour");
                conn.setAutoCommit(true);
            }catch (SQLException e){
                System.out.println("Cant reset autocommit" + e.getMessage());
            }
        }


    }

}
