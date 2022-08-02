package model;

import org.cef.misc.CefPdfPrintSettings;

import javax.xml.transform.Result;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Datasource {

    private static final String DB_NAME = "music.db";
    private static final String CONNECTION_STRING = "jdbc:sqlite:D:\\MRc\\Java\\music\\"+DB_NAME;
    private static final String TABLE_ALBUMS = "albums";
    private static final String COLUMN_ALBUM_ID = "_id";
    private static final String COLUMN_ALBUM_NAME = "name";
    private static final String COLUMN_ALBUM_ARTIST = "artist";

    private static final String TABLE_ARTISTS = "artists";
    private static final String COLUMN_ARTIST_ID = "_id";
    private static final String COLUMN_ARTIST_NAME = "name";

    private static final String TABLE_SONGS = "songs";
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
            "SELECT "+ TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME+ " FROM " + TABLE_ALBUMS +
                    " INNER JOIN "+ TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
                    " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID + " WHERE " +
                    TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + " = \"";

    /*
    query for ordering
     */
    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";


    /*
  FIRST inner join joins songs and albums where record in table SONGS has = album id to album id from
  table ALBUMS
  SECOND inner join joins table ARTISTS and ALBUMS where col.artist in ALBUMS is = to ID from ARTISTS table.

  query returns artists,their albums and the searched song from the two joins
   */
    public static final String QUERY_ARTIST_OR_SONG_START =
            "SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", "+
                    TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + ", "+
                    TABLE_SONGS + "." + COLUMN_SONG_TRACK +
                    " FROM "+
                    TABLE_SONGS +  " INNER JOIN " + TABLE_ALBUMS + " ON " +
                    TABLE_SONGS + "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
                    " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " +
                    TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +

                    " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_ARTIST_FOR_SONG_SORT =
            " ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", "
                    +TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";



    private Connection conn;

    public boolean open(){
        try {
            conn = DriverManager.getConnection(CONNECTION_STRING);
            return true;
        }
        catch (SQLException e){
            System.out.println("Couldnt connect to database" + e.getMessage());
            return false;
        }

    }

    public void close(){
        try
        {
            if (conn != null)
                conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("Couldnt close the connection "+ e.getMessage());
        }
    }

    // function returns all artists from the db
    public List<Artist> queryArtists(int orderBy){

        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(TABLE_ARTISTS);
        if (orderBy != ORDER_BY_NONE){
            sb.append(" ORDER BY ");
            sb.append(COLUMN_ARTIST_NAME);
            sb.append(" COLLATE NOCASE ");
            if(orderBy == ORDER_BY_DESC)
                sb.append("DESC");

            else sb.append("ASC");
        }

        /*
        statement and resultset in try method will be automatically closed
        wether there is an exception or not
         */
        try (Statement statement = conn.createStatement();
        ResultSet results = statement.executeQuery(sb.toString()))
        {
            List<Artist> artists = new ArrayList<>();

            while (results.next()){
                Artist artist = new Artist();
                artist.setId(results.getInt(COLUMN_ARTIST_ID));
                artist.setName(results.getString(COLUMN_ARTIST_NAME));
                artists.add(artist);
            }

            return artists;
        }

        catch (SQLException e){
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }


    }

    public List<String> queryAlbumsOfArtist (String artist, int sortOrder){

        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_ARTIST_START);
        sb.append(artist);
        sb.append("\"");

        if (sortOrder != ORDER_BY_NONE){
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_DESC)
                sb.append("DESC");
            else sb.append("ASC");
        }

        System.out.println("SQL statement = " + sb.toString());
        try (Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sb.toString())){

            List<String> albums = new ArrayList<>();
            while (rs.next()){
                albums.add(rs.getString(1));
            }
            return albums;

        } catch (SQLException e) {
            System.out.println("Query failed : " + e.getMessage());
            return null;
        }

    }


    // function returns the artist, album and the tracknumber for the required song
    public List<SongArtist> queryArtistsforSong (String songname, int sorOrder){

        StringBuilder sb = new StringBuilder(QUERY_ARTIST_OR_SONG_START);
        sb.append(songname);
        sb.append("\"");

        if (sorOrder != ORDER_BY_NONE){
            sb.append(QUERY_ARTIST_FOR_SONG_SORT);
            if (sorOrder == ORDER_BY_DESC)
                sb.append("DESC");
            else sb.append("ASC");

        }
        System.out.println("SQL Statement: " + sb.toString());

        try (Statement statement = conn.createStatement();
             ResultSet results = statement.executeQuery(sb.toString())){

            List<SongArtist> songArtists = new ArrayList<>();

            while (results.next()){
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

}
