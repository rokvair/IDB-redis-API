using System.ComponentModel.DataAnnotations;

namespace Models;

public class CoachModel
{
    [Required]
    public string First_name { get; set; }

    [Required]
    public string Last_name { get; set; }

    [Required]
    public string Nationality { get; set; }

    [Required]
    public int Experience { get; set; }

    [Required]
    public string FK_Team_Id { get; set; }
}