// jdbc:sqlite:identifier.sqlite

import model.Artist;
import model.Datasource;
import model.SongArtist;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        Datasource datasource = new Datasource();

        if (datasource.open() == false){
            System.out.println("Cant open datasource");
            return;
        }
        List<Artist> artists = datasource.queryArtists(Datasource.ORDER_BY_ASC);

        if (artists == null){
            System.out.println("No artists");
            return;
        }

        for (Artist artist : artists){
            System.out.println("ID = " + artist.getId() +
                    ", Name = "+ artist.getName());
        }

        List<String> albumsforArtist = datasource.queryAlbumsOfArtist("Iron Maiden",Datasource.ORDER_BY_ASC);

        for (String album : albumsforArtist){
            System.out.println(album);
        }

        List<SongArtist> songArtists = datasource.queryArtistsforSong("Heartless", Datasource.ORDER_BY_ASC);
        if (songArtists == null){
            System.out.println("Cant find the artist for the song");
            return;
        }
        for (SongArtist artist : songArtists){
            System.out.println("Artist name = " + artist.getArtistName()+
            " Album name = " + artist.getAlbumname() + " Track = " + artist.getTrack());
        }

        datasource.close();
    }

}
