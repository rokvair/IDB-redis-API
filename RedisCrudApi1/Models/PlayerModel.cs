using System.ComponentModel.DataAnnotations;

namespace Models;
public class PlayerModel
{
    [Required]
    public string First_name { get; set; }

    [Required]
    public string Last_name { get; set; }

    [Required]
    public int Birthyear { get; set; }

    [Required]
    public string Position { get; set; }

    [Required]
    public string Nationality { get; set; }

    [Required]
    public int Height { get; set; }

    [Required]
    public int Goals { get; set; }
    
    [Required]
    public int Assists { get; set; }

    [Required]
    public string FK_Team_Id{ get; set; }
}